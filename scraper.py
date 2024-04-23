import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

def scraper(url, resp):
    links = extract_next_links(url, resp)
    with open('report-1-and-4.txt', 'a') as file:
        for link in links:
            file.write(link + '\n')
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content

    # error checking
    if resp.status != 200:
        print(resp.error)
        return list()
    elif not resp.raw_response.content:
        return list()
    
    # decode bytes to html
    decoded = resp.raw_response.content.decode("utf-8", errors="ignore")
    
    # parse for href
    soup = BeautifulSoup(decoded, 'html.parser')

    # get base url in case of relative urls found
    base_url = resp.url

    # FILTER OUT: large & small files
    text = soup.get_text()
    text_length = len(text)
    # print("text length: ", text_length)
    # TODO: check for outlying large sizes & small sizes
    with open("length_threshold.txt", "a") as file:
        file.write(str(text_length)+"\n")

    # FILTER OUT: low information
    html_length = len(soup.prettify())
    ratio = text_length / html_length
    # print("ratio: ", ratio)
    # TODO: check for low ratios
    with open("ratio_threshold.txt", "a") as file:
        file.write(str(ratio)+"\n")

    # FILTER OUT: similar pages w/ simhashing
    


    final_links = []
    links = soup.find_all("a")
    for link in links:
        href = link.get("href")
        if href != "#" and href is not None:
            absolute_link = href
            # check if the link is already an absolute URL
            if not urlparse(href).scheme:
                # convert relative link to absolute link
                absolute_link = urljoin(base_url, href)
            final_links.append(absolute_link)
    # add into the buffer
    
    return final_links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    # TODO: might need to filter out more invalid extensions
    
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        # TODO: transform relative to absolute urls


        # FILTER OUT: non ics.uci.edu domains
        domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]
        if not any(domain in parsed.netloc for domain in domains):
            return False
        # if it's already in the buffer.txt then we also skip
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        # print ("TypeError for ", parsed)
        raise
