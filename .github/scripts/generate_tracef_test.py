#!/usr/bin/env python3
"""
Generate a C test file that exercises all dqlite_tracef calls found in the source code.
"""

GEN_FILE_PROLOGUE = """
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>

#define dqlite_tracef dqlite_tracef_real
#include "src/tracing.c"
#undef dqlite_tracef

#include "src/raft.h"

FILE *check_file;
int test_file;

void dqlite_tracef(const char *file, unsigned int line, const char *func, unsigned int level, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  vfprintf(check_file, fmt, args);
  putc('\\n', check_file);
  va_end(args);
}

static uint64_t test_seed;

unsigned long generate_unsigned_long() {
  return (unsigned long)test_seed;
}

int generate_int() {
  return (int)test_seed;
}

pid_t generate_pid_t() {
  return (pid_t)(test_seed % 32768);
}

uint32_t generate_uint32_t() {
  return (uint32_t)test_seed;
}

int32_t generate_int32_t() {
  return (int32_t)test_seed;
}

uint64_t generate_uint64_t() {
  return (uint64_t)test_seed;
}

int64_t generate_int64_t() {
  return (int64_t)test_seed;
}

unsigned int generate_unsigned_int() {
  return (unsigned int)test_seed;
}

void* generate_void_ptr() {
  return (void*)test_seed;
}

raft_id generate_raft_id() {
  return (raft_id)test_seed;
}

char* generate_char_ptr() {
  static char buffer[20];
  static bool initializer = false;
  if (!initializer) {
    snprintf(buffer, sizeof(buffer), "str_%d", generate_int());
    initializer = true;
  }
  return buffer;
}

const char* generate_const_char_ptr() {
  return generate_char_ptr();
}

raft_index generate_raft_index() {
  return (raft_index)test_seed;
}

raft_term generate_raft_term() {
  return (raft_term)test_seed;
}

raft_time generate_raft_time() {
  return (raft_time)test_seed;
}

int main() {
  _dqliteTracingEnabled = true;
  srand(time(NULL));
  test_seed = ((uint64_t)rand()) << 32 | (uint64_t)rand();

  check_file = fopen("check.txt", "w");
  test_file = open("test.txt", O_CREAT | O_WRONLY | O_TRUNC, 0664);
"""

GEN_FILE_EPILOGUE = """
  dqlite_print_crash_trace(test_file);
  return 0;
}"""

import subprocess
import json
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_string(node):
    """Extract string value from AST node."""
    if node.get('kind') == 'StringLiteral':
        return node.get('value')
    if 'inner' in node and len(node['inner']) > 0:
        return get_string(node['inner'][0])
    raise Exception('cannot extract format from trace call')

def is_dqlite_tracef_call(node):
    """Check if AST node represents a dqlite_tracef function call."""
    if node.get('kind') != 'CallExpr':
        return False
    
    callee = node.get('inner', [None])[0]
    while callee and callee.get('kind') != 'DeclRefExpr':
        if 'inner' in callee and len(callee['inner']) > 0:
            callee = callee['inner'][0]
        else:
            return False
    
    return (callee and 
            callee.get('kind') == 'DeclRefExpr' and 
            callee.get('referencedDecl', {}).get('name') == 'dqlite_tracef')

def process_source_file(source):
    """Process a single C source file to extract tracef calls."""
    cmd = f"clang -D_GNU_SOURCE -Xclang -ast-dump=json -fsyntax-only {source} | jq -c '.. | objects | select(.kind == \"CallExpr\")'"
    
    tracef_calls = []
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            try:
                node = json.loads(line)
                if is_dqlite_tracef_call(node):
                    format_str = get_string(node['inner'][5])
                    args = []
                    for n in node['inner'][6:]:
                        qual_type = n.get('type', {}).get('qualType', '')
                        generator = 'generate_' + qual_type.replace('*', 'ptr').replace(' ', '_')
                        args.append(f"{generator}()")
                    
                    all_args = [format_str] + args
                    tracef_calls.append(f"tracef({', '.join(all_args)}); // from {source}\n")
            except json.JSONDecodeError:
                continue
    except Exception as e:
        print(f"Error processing {source}: {e}")
        raise
    
    return tracef_calls

def main():
    # Write C file header
    with open("debug_trace_generated.c", "w") as output:
        output.write(GEN_FILE_PROLOGUE)
    
        # Find all C source files
        sources = glob.glob("src/**/*.c", recursive=True)
        total = len(sources)
        
        # Process files in parallel
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(process_source_file, source): source for source in sources}
            remaining = total
            for future in as_completed(futures):
                source = futures[future]
                remaining = remaining - 1
                print(f"Processed {source}... {remaining} remaining.")
                
                try:
                    # Write all tracef calls and footer
                    calls = future.result()
                    for call in calls:
                        output.write(f"  {call}")
                except Exception as e:
                    print(f"Error processing {source}: {e}")
        
        
        output.write(GEN_FILE_EPILOGUE)

if __name__ == "__main__":
    main()
