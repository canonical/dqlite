#!/bin/sh

ROW_COUNT=1000
WAL_AUTOCHECKPOINT=0
MAIN_ONLY=0
usage() {
	cat <<USAGE
Usage: generate-snapshot.sh [--main-only] [--wal-limit <num>] [--row-count <num>] <output-dir>

Options:
  --main-only         Generate a main database only snapshot (no WAL)
  --wal-limit <num>   Maximum amount of WAL pages to keep (default: 0 - unbounded)
  --row-count <num>   Number of rows to generate (default: 1000)
  -h, --help          Show this help message

USAGE
	exit 0
}

# Parse arguments
while [ $# -gt 0 ]; do
	case "$1" in
        --main-only)
            MAIN_ONLY=1
            shift 1
            ;;
		--wal-limit)
			if [ -z "$2" ]; then
				echo "Error: --wal-limit requires a value" >&2
				usage
			fi
			WAL_AUTOCHECKPOINT="$2"
			shift 2
			;;
		--row-count)
			if [ -z "$2" ]; then
				echo "Error: --row-count requires a value" >&2
				usage
			fi
			ROW_COUNT="$2"
			shift 2
			;;
		-h|--help)
			usage
			;;
		*)
			if [ -n "$2" ]; then
                echo "Error: unknown option '$1'" >&2
                usage
            fi
            OUTPUT_DIR="$1"
            shift 1
			;;
	esac
done

if [ -z "$OUTPUT_DIR" ]; then
	echo "Error: Missing output directory" >&2
	usage
fi

# Cleanup
trap 'rm -f temp temp-wal temp-shm' EXIT

SQLITE3_CHECKPOINT_ON_CLOSE=""
if [ "$MAIN_ONLY" -eq 0 ]; then
    SQLITE3_CHECKPOINT_ON_CLOSE=".dbconfig no_ckpt_on_close on"
fi

# First generate a sqlite3 database
echo -n "Generating database with $ROW_COUNT rows..."
cat <<EOF | sqlite3 temp > /dev/null 2>&1
PRAGMA journal_mode=WAL;
PRAGMA wal_autocheckpoint=$WAL_AUTOCHECKPOINT;
$SQLITE3_CHECKPOINT_ON_CLOSE

CREATE TABLE test(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
WITH sequence AS (
   SELECT 1 AS id

   UNION ALL

   SELECT id + 1
   FROM sequence
   WHERE id < $ROW_COUNT
)
INSERT OR REPLACE INTO test
SELECT id, hex(randomblob(16))
FROM sequence;

EOF
echo "Done"


# Now generate a dqlite snapshot from the sqlite3 database
echo -n "Generating snapshot in '$OUTPUT_DIR'..."
cat <<EOF | dqlite-utils > /dev/null 2>&1
.snapshot
.add-server "1"
ATTACH DATABASE "temp" AS test;
.finish "$OUTPUT_DIR"
EOF
echo "Done"
