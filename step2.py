response = requests.get('https://raw.githubusercontent.com/run-llama/llama_index/main/docs/docs/examples/data/paul_graham/paul_graham_essay.txt')
text = response.text
f = open('essay.txt', 'w')
f.write(text)
f.close()
len(text)
