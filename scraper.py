import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import hashlib
import json
import numpy as np
from collections import defaultdict

def scraper(url, resp, small_buffer):
    links = extract_next_links(url, resp, small_buffer)
    if(links != []):
        small_buffer.append((url, resp))
    if(len(links) > 5):
        small_buffer.pop(0)
    
    # prep output for report 2
    decoded = resp.raw_response.content.decode("utf-8", errors="ignore")
    soup = BeautifulSoup(decoded, 'html.parser')
    text = soup.get_text()
    numWords = len(findWords(text))
    with open("report-2.txt", "a") as file:
        file.write(url + " " + str(numWords)+"\n")
    
    # prep output for report 3
    update_frequencies(text)
    return [link for link in links if is_valid(link)], small_buffer
    #return [link for link in links if is_valid(link)]


def extract_next_links(url, resp, small_buffer):
    #every time extract is called, a buffer for the current url, save the last five pages
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
    with open("length_threshold.txt", "a") as file:
        file.write(str(text_length)+"\n")
    if text_length < 100 or text_length > 30000:
        print(f"Filtering out {url} bc file too large/small")
        return list()

    # FILTER OUT: low information
    html_length = len(soup.prettify())
    ratio = text_length / html_length
    with open("ratio_threshold.txt", "a") as file:
        file.write(str(ratio)+"\n")
    if ratio <= 0.03:
        print(f"Filtering out {url} bc low info")
        return list()

    # FILTER OUT: similar pages w/ simhashing
    currWeight = findWeights(text)
    currFingerprint = generate_fingerprint(currWeight)
    for link, r in small_buffer:
        decoded2 = r.raw_response.content.decode("utf-8", errors="ignore")
        soup2 = BeautifulSoup(decoded2, 'html.parser')
        text2 = soup2.get_text()
        prevWeight = findWeights(text2)
        prevFingerprint = generate_fingerprint(prevWeight)
        if similarity(currFingerprint, prevFingerprint) >= (31/32):
            print(f"Filtering out {url} bc too similar")
            return list()


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
    return final_links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    # TODO: might need to filter out more invalid extensions
    
    try:
        url = url.replace('\u200E', '')
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False

        # TODO: transform relative to absolute urls


        # FILTER OUT: non ics.uci.edu domains
        domains = ["ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"]
        if not any(domain in parsed.netloc for domain in domains):
            return False

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


def findWords(text):
    '''
    Finds all the words on the page, accounts for ' and -
    Used for report q2 output
    '''
    all_tokens = []
    token = ""
    for c in text:
        if (('A' <= c <= 'Z') or ('a' <= c <= 'z') or ('0' <= c <= '9') or (c == "'") or (c == "-")):
            token += c
        else:
            if token:   # if token not empty, add token
                all_tokens.append(token.lower())
                token = ""
    
    if token:   # add last token
        all_tokens.append(token.lower())
    
    return all_tokens


def wordFrequencies(text):
    '''
    Finds all the frequencies of words, accounts for ' and -
    Used for report q3 output
    '''
    all_tokens = []
    token = ""
    for c in text:
        if (('A' <= c <= 'Z') or ('a' <= c <= 'z') or ('0' <= c <= '9') or (c == "'") or (c == "-")):
            token += c
        else:
            if token:   # if token not empty, add token
                all_tokens.append(token.lower())
                token = ""
    
    if token:   # add last token
        all_tokens.append(token.lower())
    
    map = defaultdict(int)
    # loop through each token and increment its counter in the map

    # stop words
    stopwords = [
        "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
        "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
        "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
        "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's",
        "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
        "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't",
        "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
        "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so",
        "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
        "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too",
        "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what",
        "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with",
        "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
    ]

    for token in all_tokens:
        if token not in stopwords or len(token) > 1:
            map[token] += 1
    
    return dict(map)


def update_frequencies(text):
    '''
    Updates json file with new frequencies
    Used for report q3 output
    '''
    try:
        with open("report-3.json", "r") as file:
            old_data = json.load(file)
    except:
        old_data = {}
    
    frequencies = wordFrequencies(text)

    for word, count in frequencies.items():
        if word in old_data:
            old_data[word] += count
        else:
            old_data[word] = count
    
    with open("report-3.json", "w") as file:
        json.dump(old_data, file, indent=4)


def findWeights(text):
    '''
    Finds the frequency of each token in a page
    '''
    all_tokens = []
    token = ""
    for c in text:
        if (('A' <= c <= 'Z') or ('a' <= c <= 'z') or ('0' <= c <= '9')):
            token += c
        else:
            if token:   # if token not empty, add token
                all_tokens.append(token.lower())
                token = ""
    
    if token:   # add last token
        all_tokens.append(token.lower())
    
    map = defaultdict(int)
    # loop through each token and increment its counter in the map
    for token in all_tokens:
        map[token] += 1
    
    return dict(map)


def generate_fingerprint(weights):
    '''
    Generates a fingerprint for simhashing purposes
    '''
    # initialize Vector V
    V = np.zeros(32, dtype=int)

    for word, weight in weights.items():
        # generate hash value
        hash_value = hashlib.sha256(word.encode()).digest()[:4]
        hash = ''.join(f"{byte:08b}" for byte in hash_value)

        # create vector V
        for i, bit in enumerate(hash):
            if bit == '1':
                V[i] += weight
            else:
                V[i] -= weight

    # generate fingerprint
    fingerprint = np.where(V > 0, 1, 0)
    return fingerprint


def similarity(fingerprint1, fingerprint2):
    '''
    Compares 2 fingerprints and generate a similarity score
    '''
    same_bits = sum(b1 == b2 for b1, b2 in zip(fingerprint1, fingerprint2))

    return same_bits / 32.0