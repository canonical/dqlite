#include "addrinfo.h"

#include <uv.h>

#include <dlfcn.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

bool addrinfo_mock_enabled = false;

enum addrinfo_mock_state { MockResultSet, MockResultReturned, SystemResult };

struct addrinfo_mock_data
{
    enum addrinfo_mock_state state;
    int rv;
    struct addrinfo *result;
    struct addrinfo_mock_data *next;
};

static struct addrinfo_mock_data *addrinfo_data;

void AddrinfoInjectSetUp(MUNIT_UNUSED const MunitParameter params[])
{
    munit_assert_int(addrinfo_mock_enabled, ==, false);
    munit_assert_ptr((void *)addrinfo_data, ==, NULL);
    addrinfo_mock_enabled = true;
}

void AddrinfoInjectTearDown(void)
{
    munit_assert_int(addrinfo_mock_enabled, ==, true);
    // If data is not freed the freeaddrinfo was not invoked.
    munit_assert_ptr((void *)addrinfo_data, ==, NULL);
    addrinfo_mock_enabled = false;
}

void AddrinfoInjectSetResponse(int rv,
                               int num_results,
                               const struct AddrinfoResult *results)
{
    munit_assert_int(addrinfo_mock_enabled, ==, true);
    munit_assert(!addrinfo_data || addrinfo_data->state == MockResultReturned);
    munit_assert(rv || (num_results && results));

    struct addrinfo_mock_data *response =
        malloc(sizeof(struct addrinfo_mock_data));
    munit_assert_ptr((void *)response, !=, NULL);
    response->state = MockResultSet;
    response->rv = rv;
    response->result = NULL;
    for (int i = num_results - 1; i >= 0; --i) {
        struct sockaddr_in *addr_in = malloc(sizeof(struct sockaddr_in));
        munit_assert_ptr((void *)addr_in, !=, NULL);
        munit_assert_int(uv_ip4_addr(results[i].ip, results[i].port, addr_in),
                         ==, 0);

        struct addrinfo *ai = malloc(sizeof(struct addrinfo));
        munit_assert_ptr((void *)ai, !=, NULL);
        ai->ai_flags = 0;
        ai->ai_family = AF_INET;
        ai->ai_socktype = SOCK_STREAM;
        ai->ai_protocol = IPPROTO_TCP;
        ai->ai_addrlen = sizeof(struct sockaddr_in);
        ai->ai_addr = (struct sockaddr *)addr_in;
        ai->ai_canonname = NULL;
        ai->ai_next = response->result;
        response->result = ai;
    }
    response->next = addrinfo_data;
    addrinfo_data = response;
}

static int invoke_system_getaddrinfo(const char *node,
                                     const char *service,
                                     const struct addrinfo *hints,
                                     struct addrinfo **res)
{
    int (*system_getaddrinfo)(const char *node, const char *service,
                              const struct addrinfo *hints,
                              struct addrinfo **res);
    *(void **)(&system_getaddrinfo) = dlsym(RTLD_NEXT, "getaddrinfo");
    munit_assert_ptr(*(void **)&system_getaddrinfo, !=, NULL);
    return (*system_getaddrinfo)(node, service, hints, res);
}

int getaddrinfo(const char *node,
                const char *service,
                const struct addrinfo *hints,
                struct addrinfo **res)
{
    int rv;

    if (!addrinfo_mock_enabled) {
        return invoke_system_getaddrinfo(node, service, hints, res);
    }
    if (!addrinfo_data || addrinfo_data->state == SystemResult) {
        /* We have not injected response, invoke system function */
        rv = invoke_system_getaddrinfo(node, service, hints, res);
        if (!rv) {
            /* Store result for check on freeaddrinfo */
            struct addrinfo_mock_data *response =
                malloc(sizeof(struct addrinfo_mock_data));
            munit_assert_ptr((void *)response, !=, NULL);
            response->state = SystemResult;
            response->rv = rv;
            response->result = *res;
            response->next = addrinfo_data;
            addrinfo_data = response;
        }
        return rv;
    }
    if (addrinfo_data) {
        munit_assert_int(addrinfo_data->state, ==, MockResultSet);
        addrinfo_data->state = MockResultReturned;
        rv = addrinfo_data->rv;
        if (!rv) {
            *res = addrinfo_data->result;
        } else {
            *res = NULL;
            struct addrinfo_mock_data *response = addrinfo_data;
            munit_assert_ptr((void *)response->result, ==, NULL);
            addrinfo_data = response->next;
            free(response);
        }
        return rv;
    }
    return EAI_FAIL;
}

static void invoke_system_freeaddrinfo(struct addrinfo *res)
{
    int (*system_freeaddrinfo)(struct addrinfo * res);
    *(void **)(&system_freeaddrinfo) = dlsym(RTLD_NEXT, "freeaddrinfo");
    munit_assert_ptr(*(void **)&system_freeaddrinfo, !=, NULL);
    (*system_freeaddrinfo)(res);
}

void freeaddrinfo(struct addrinfo *res)
{
    struct addrinfo_mock_data **ptr;
    struct addrinfo_mock_data *response;

    // freeaddrinfo should not be invoked with a NULL pointer
    munit_assert_ptr((void *)res, !=, NULL);

    if (!addrinfo_mock_enabled) {
        invoke_system_freeaddrinfo(res);
        return;
    }
    for (ptr = &addrinfo_data; *ptr; ptr = &((*ptr)->next)) {
        if ((*ptr)->result == res) {
            break;
        }
    }
    response = *ptr;
    munit_assert_ptr((void *)response, !=, NULL);
    *ptr = response->next;
    if (response->state == SystemResult) {
        invoke_system_freeaddrinfo(response->result);
    } else {
        munit_assert_int(response->state, ==, MockResultReturned);
        res = response->result;
        while (res) {
            struct addrinfo *next = res->ai_next;
            free(res->ai_addr);
            free(res);
            res = next;
        }
    }
    free(response);
}
