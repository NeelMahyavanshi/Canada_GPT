import os
import json

# Output file
output_file = "all_canada_rag_content.jsonl"

# List all .jsonl files in current directory
jsonl_files = [f for f in os.listdir(".") if f.endswith(".jsonl")]

if not jsonl_files:
    print("❌ No .jsonl files found.")
else:
    print(f"📁 Found {len(jsonl_files)} files to combine.")

    with open(output_file, "w", encoding="utf-8") as outfile:
        for filename in jsonl_files:
            print(f"🔄 Processing: {filename}")
            try:
                with open(filename, "r", encoding="utf-8") as infile:
                    for line in infile:
                        line = line.strip()
                        if line:
                            # Validate and write JSON line
                            try:
                                obj = json.loads(line)
                                outfile.write(line + "\n")
                            except json.JSONDecodeError as e:
                                print(f"⚠️  Skipping invalid JSON in {filename}: {e}")
                                continue
            except Exception as e:
                print(f"❌ Error reading {filename}: {e}")

    print(f"\n✅ Combined {len(jsonl_files)} files into {output_file}")