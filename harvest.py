#
# Harvest metadata from source.
#

import typer

def main(source: str):
    print(f"Source: {source}")

if __name__ == "__main__":
    typer.run(main)

