#listing contents of each file in directory
from pathlib import Path

current_dir = Path.cwd() # btw same as Path(".")
current_file = Path(__file__).name

print(f"Current file name is {current_file}")
print(f"Files in {current_dir}:")

for filepath in current_dir.iterdir():  #iterdir for all the files in cwd
    if filepath.name == current_file:
        continue    # skippin script

    print(f"  - {filepath.name}")

    if filepath.is_file():
        content = filepath.read_text()
        print(f"    Content: {content}")
