import shelve

if __name__ == "__main__":
    temp = shelve.open("subdomains.shelve")
    for url, num in temp.values():
        print(url, num)