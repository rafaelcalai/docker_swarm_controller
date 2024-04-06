import sys
import time


def wait_for_value(value):
    print(f"Waiting for: {value}s")
    time.sleep(int(value))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python wait_for_value.py <value>")
        sys.exit(1)

    value = sys.argv[1]
    wait_for_value(value)
