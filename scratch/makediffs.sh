#!/bin/bash

rm *.diff

# Ensure the script is run from the directory containing the .aossdump and .esdump files
# Or provide the directory path as an argument

# Check if a directory is provided as an argument
if [ $# -eq 1 ]; then
  DIR=$1
  # Change to the provided directory
  cd "$DIR" || { echo "Directory not found: $DIR"; exit 1; }
else
  # Use the current directory if no argument is provided
  DIR="."
fi

# Iterate through all .aossdump files in the directory
for aossdump_file in "$DIR"/*.aossdump; do
  # Check if the file exists (to handle the case where there are no .aossdump files)
  if [ ! -f "$aossdump_file" ]; then
    echo "No .aossdump files found in the directory."
    exit 1
  fi

  # Get the base name without the extension
  base_name=$(basename "$aossdump_file" .aossdump)

  # Construct the corresponding .esdump file name
  esdump_file="$DIR/$base_name.esdump"

  # Check if the corresponding .esdump file exists
  if [ -f "$esdump_file" ]; then
    # Construct the .diff file name
    diff_file="$DIR/$base_name.diff"

    # Run diff and save the output to the .diff file
    diff "$aossdump_file" "$esdump_file" > "$diff_file"

    # Optional: Output a message indicating the diff was created
    echo "Diff created: $diff_file"
  else
    echo "Warning: Corresponding .esdump file not found for $aossdump_file"
  fi
done

echo "Diff operation completed."
