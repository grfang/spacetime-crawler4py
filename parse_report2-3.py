import json

# parse report2
longest_page = ""
longest_count = 0

with open("report-2.txt") as file:
    for line in file:
        page, count = line.rsplit(' ', 1)
        if int(count) > longest_count:
            longest_page = page
            longest_count = int(count)

print(longest_page)
print(longest_count)


# parse report3
with open("report-3.json", "r") as file:
    data = json.load(file)

# sort by descending
sorted_items = sorted(data.items(), key=lambda x:x[1], reverse=True)

top50 = sorted_items[:50]

for word, count in top50:
    print(word, count)
        