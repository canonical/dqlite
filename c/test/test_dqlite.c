#include <assert.h>
#include <pthread.h>

#include <CUnit/CUnit.h>
#include <sqlite3.h>

#include "dqlite.h"
#include "suite.h"
#include "server.h"
#include "client.h"
#include "cluster.h"

static dqlite_service *testInstance = 0;

void test_dqlite_create(){
  int err;
  FILE *log = test_suite_dqlite_log();

  testInstance = dqlite_service_alloc();
  CU_ASSERT_PTR_NOT_NULL( testInstance );

  err = dqlite_service_init(testInstance, log, test_cluster());

  CU_ASSERT_EQUAL(err, 0);
}

void test_dqlite_destroy(){
  dqlite_service_close(testInstance);
  dqlite_service_free(testInstance);
  testInstance = 0;
}

static test_server* server = 0;
static struct test_client *client = 0;

int dqlite_loop_init(){
  int err;

  assert( !server );
  assert( !client );

  server = test_server_start();
  if( !server ){
    return 1;
  }

  err = test_server_connect(server, &client);
  if( err ){
    return 1;
    }

  return 0;
}

int dqlite_loop_cleanup(){
  int err;

  assert( server );
  assert( client );

  test_client_close(client);

  err = test_server_stop(server);
  server = 0;

  if( err ){
    return err;
  }

  return 0;
}

void test_dqlite_start(){
  int err;
  char *leader;

  err = test_client_handshake(client);
  CU_ASSERT_EQUAL(err, 0);

  err = test_client_leader(client, &leader);
  CU_ASSERT_EQUAL(err, 0);
}

void test_dqlite_stop(){
  //  CU_ASSERT_EQUAL(1, 0);
}
