#include <assert.h>
#include <pthread.h>

#include <CUnit/CUnit.h>
#include <sqlite3.h>

#include "dqlite.h"
#include "suite.h"
#include "server.h"
#include "client.h"
#include "cluster.h"

static dqlite_server *testInstance = 0;

void test_dqlite_create(){
  int err;
  FILE *log = test_suite_dqlite_log();

  testInstance = dqlite_server_alloc();
  CU_ASSERT_PTR_NOT_NULL( testInstance );

  err = dqlite_server_init(testInstance, log, test_cluster());

  CU_ASSERT_EQUAL(err, 0);
}

void test_dqlite_destroy(){
  dqlite_server_close(testInstance);
  dqlite_server_free(testInstance);
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
  uint64_t heartbeat;
  uint32_t db_id;
  uint32_t stmt_id;

  err = test_client_handshake(client);
  CU_ASSERT_EQUAL(err, 0);

  err = test_client_leader(client, &leader);
  CU_ASSERT_EQUAL(err, 0);

  err = test_client_client(client, &heartbeat);
  CU_ASSERT_EQUAL(err, 0);

  err = test_client_open(client, "test.db", &db_id);
  CU_ASSERT_EQUAL(err, 0);

  CU_ASSERT_EQUAL(db_id, 0);

  err = test_client_prepare(client, db_id, "CREATE TABLE test (n INT)", &stmt_id);
  CU_ASSERT_EQUAL(err, 0);

  CU_ASSERT_EQUAL(stmt_id, 0);

  err = test_client_exec(client, db_id, stmt_id);
  CU_ASSERT_EQUAL(err, 0);

  err = test_client_finalize(client, db_id, stmt_id);
  CU_ASSERT_EQUAL(err, 0);

  err = test_client_prepare(client, db_id, "INSERT INTO test VALUES(123)", &stmt_id);
  CU_ASSERT_EQUAL(err, 0);

  CU_ASSERT_EQUAL(stmt_id, 0);

  err = test_client_exec(client, db_id, stmt_id);
  CU_ASSERT_EQUAL(err, 0);

  err = test_client_finalize(client, db_id, stmt_id);
  CU_ASSERT_EQUAL(err, 0);

  err = test_client_prepare(client, db_id, "SELECT n FROM test", &stmt_id);
  CU_ASSERT_EQUAL(err, 0);

  CU_ASSERT_EQUAL(stmt_id, 0);

  err = test_client_query(client, db_id, stmt_id);
  CU_ASSERT_EQUAL(err, 0);

  err = test_client_finalize(client, db_id, stmt_id);
  CU_ASSERT_EQUAL(err, 0);
}

void test_dqlite_stop(){
}
