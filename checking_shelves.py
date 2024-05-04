import shelve

if __name__ == "__main__":
    # url = "https://gitlab.com/aosp-mirror-1/platform/external/tagsoup/-/tree/main"
    # if 'gitlab' in url:
    #     print("LOL")
    # temp = shelve.open("frontier.shelve")
    # print(len(temp))
    # sorted_items = sorted(temp.values(), key=lambda x: x[2])
    # for url, completed, in sorted_items:
    #     print(url)


    # temp = shelve.open("unique.shelve")
    # print(len(temp))
    # sorted_items = sorted(temp.values())
    # for url in sorted_items:
    #     print(url)

    temp = shelve.open("subdomains.shelve")
    print(len(temp))
    sorted_items = sorted(temp.values(), key=lambda x: x[0])
    for url, num in sorted_items:
        print(url, num)
    pass