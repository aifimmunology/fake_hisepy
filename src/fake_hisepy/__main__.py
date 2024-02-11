import argparse

def main():
    parser = argparse.ArgumentParser(description="This script provides information about the program.")
    parser.add_argument('--version', action='store_true', help='Show the version of the program')
    parser.add_argument('--about', action='store_true', help='Show information about the program')

    args = parser.parse_args()
    message = None

    if args.version:
        message = "Program Version 1.0"
    elif args.about:
        message = "This is a program that provides information based on command line arguments."

    print(message)
    return message

if __name__ == "__main__":
    result = main()
    if result:
        print(result)

