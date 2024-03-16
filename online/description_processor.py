import re

def keywords_finder(corpus: str, keyword_dict: dict) -> str:
    """
    find keywords in the corpus
    :param corpus: text corpus
    :param keyword_dict: dictionary of keywords
    :return: a string of keywords found in the corpus separated by :
    """
    found_keywords = []
    # get words from corpus and convert to lower case
    corpus = re.sub(r'[^\w\s]', '', corpus).lower().split()
    for word in corpus:
        if word in keyword_dict.keys():
            found_keywords.append(keyword_dict[word])
    if len(found_keywords) > 0:
        found_keywords = ':'.join(set(found_keywords))
    else:
        found_keywords = ''
    return found_keywords
