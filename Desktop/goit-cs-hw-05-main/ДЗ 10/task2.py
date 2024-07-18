import requests
from collections import Counter
import matplotlib.pyplot as plt
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

def map_reduce(input_data, mapper, reducer, num_threads=4):
    mapped = []
    

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(mapper, item) for item in input_data]
        
        
        for future in as_completed(futures):
            mapped.extend(future.result())
    
    grouped = {}
    for key, value in mapped:
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(value)
    
    reduced = []
    for key, values in grouped.items():
        reduced.append((key, reducer(values)))
    
    return reduced

def mapper(text):
    words = re.findall(r'\w+', text.lower())
    return [(word, 1) for word in words]

def reducer(values):
    return sum(values)

def visualize_top_words(word_counts, top_n=10):
    top_words = dict(Counter(dict(word_counts)).most_common(top_n))
    plt.bar(top_words.keys(), top_words.values())
    plt.xlabel('Words')
    plt.ylabel('Frequency')
    plt.title('Top Words by Frequency')
    plt.show()

if __name__ == "__main__":
    url = 'https://www.gutenberg.org/files/64317/64317-0.txt'  
    response = requests.get(url)
    text = response.text
    
    chunk_size = len(text) // 4
    chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

    word_counts = map_reduce(chunks, mapper, reducer)
    visualize_top_words(word_counts)
