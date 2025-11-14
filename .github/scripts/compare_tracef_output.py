#!/usr/bin/env python3
"""
Validate that test.txt trace output matches expected format
compared to check.txt reference output.
"""

import re
import sys

def validate_trace_output(check_lines, test_lines):
    """Validate the trace output format and content."""
    errors = []
    message_count = 0
    
    # Check header line
    if len(test_lines) == 0:
        errors.append('Test file is empty')
        return {'success': False, 'errors': errors, 'message_count': 0}
    
    header_line = test_lines[0]
    header_match = re.match(r'Tentatively showing last (\d+) crash trace records:', header_line)
    if not header_match:
        errors.append(f'Invalid header format: {header_line}')
        return {'success': False, 'errors': errors, 'message_count': 0}
    
    expected_trace_count = int(header_match.group(1))
    actual_trace_lines = test_lines[1:]
    
    if expected_trace_count != len(check_lines):
        errors.append(f'Header says {expected_trace_count} records but check.txt has {len(check_lines)} lines')
    
    if len(actual_trace_lines) != len(check_lines):
        errors.append(f'Expected {len(check_lines)} trace lines but got {len(actual_trace_lines)}')
        return {'success': False, 'errors': errors, 'message_count': 0}
    
    previous_timestamp = 0
    expected_pid = None
    expected_line = None
    
    for i, (trace_line, check_message) in enumerate(zip(actual_trace_lines, check_lines)):
        trace_match = re.match(r'^\s*(\d+)\s+(\d+)\s+([^:]+):(\d+)\s+(\w+)\s+(.*)$', trace_line)
        
        if not trace_match:
            errors.append(f'Line {i + 2}: Invalid trace format: {trace_line}')
            continue
        
        timestamp, pid, filename, line_num, function_name, message = trace_match.groups()
        
        current_timestamp = int(timestamp)
        if current_timestamp < previous_timestamp:
            errors.append(f'Line {i + 2}: Timestamp {current_timestamp} is not greater than previous {previous_timestamp}')
        previous_timestamp = current_timestamp
        
        if expected_pid is None:
            expected_pid = pid
        elif pid != expected_pid:
            errors.append(f'Line {i + 2}: PID {pid} differs from expected {expected_pid}')
        
        if filename != 'debug_trace_generated.c':
            errors.append(f'Line {i + 2}: Filename \'{filename}\' should be \'debug_trace_generated.c\'')
        
        current_line_num = int(line_num)
        if expected_line is None:
            expected_line = current_line_num
        if current_line_num != expected_line:
            errors.append(f'Line {i + 2}: Line number {current_line_num} should be {expected_line}')
        expected_line += 1
        
        if function_name != 'main':
            errors.append(f'Line {i + 2}: Function \'{function_name}\' should be \'main\'')
        
        clean_message = message.strip()
        clean_check_message = check_message.strip()
        if clean_message != clean_check_message:
            errors.append(f'Line {i + 2}: Message mismatch')
            errors.append(f'  Expected: "{clean_check_message}"')
            errors.append(f'  Got:      "{clean_message}"')
        
        message_count += 1
    
    return {
        'success': len(errors) == 0,
        'errors': errors,
        'message_count': message_count
    }

def main():
    check_file = 'check.txt'
    test_file = 'test.txt'
    
    try:
        # Read both files
        with open(check_file, 'r') as f:
            check_lines = f.read().strip().split('\n')
        
        with open(test_file, 'r') as f:
            test_lines = f.read().strip().split('\n')
        
        print(f'Check file has {len(check_lines)} lines')
        print(f'Test file has {len(test_lines)} lines')
        
        # Validate test file structure
        result = validate_trace_output(check_lines, test_lines)
        
        if result['success']:
            print('✅ All validation checks passed!')
            print(f'Validated {result["message_count"]} trace messages')
        else:
            print('❌ Validation failed:', file=sys.stderr)
            for error in result['errors']:
                print(f'  - {error}', file=sys.stderr)
            sys.exit(1)
    
    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
