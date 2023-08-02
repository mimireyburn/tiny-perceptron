import psycopg2
from transformers import BertModel, BertTokenizer
from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import torch  # You need to import PyTorch
from sklearn.feature_extraction.text import CountVectorizer

# Function to connect to your PostgreSQL database
def create_conn():
    conn = psycopg2.connect(host="65.109.54.241", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    return conn

# Function to fetch data from your database
def fetch_data(conn):
    cur = conn.cursor()
    cur.execute("SELECT item_key, content, language, topics, sentiment, genre FROM public.item_txt_content WHERE language = 'english' AND sentiment = 'funny' AND genre = 'humorous comedy' AND topics LIKE '%%ball%%' ")
    rows = cur.fetchall()
    return rows

# Create connection
conn = create_conn()

# Fetch data
data = fetch_data(conn)

# Load pre-trained model tokenizer (vocabulary)
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')

# Load pre-trained model (weights)
model = BertModel.from_pretrained('bert-base-uncased')

# Initialize an empty DataFrame
df_list = []

embeddings = []

for row in data:
    item_key, content, language, topics, sentiment, genre = row

    # We tokenize our content
    tokens = tokenizer.tokenize(content)

    # Check if length of tokens exceeds 512
    if len(tokens) > 512:
        # If so, truncate to the first 512 tokens
        tokens = tokens[:512]
    
    # Convert tokens to their IDs
    input_ids = tokenizer.convert_tokens_to_ids(tokens)

    # Convert list of input_ids to tensors
    input_ids = torch.tensor([input_ids])

    # Pass it through the model to get the embedding
    output = model(input_ids)

    sentence_embedding = output[1][0].detach().numpy()  # Convert tensor to numpy array
    embeddings.append(sentence_embedding) # appending to a list to stack later

    # Add the item_key and content to a list of DataFrame
    df_list.append(pd.DataFrame({
        'item_key': [item_key],
        'content': [content],
        'topics': [topics],  # Added line
        'sentiment': [sentiment],  # Added line
        'genre': [genre]  # Added line
    }))

# Concatenate all DataFrames in the list
df = pd.concat(df_list, ignore_index=True)

# Stack embeddings and perform clustering
embeddings = np.vstack(embeddings)
kmeans = KMeans(n_clusters=10, random_state=0).fit(embeddings)

# Assign the cluster labels to each row in the DataFrame
df['cluster_label'] = kmeans.labels_

# Close connection
conn.close()

# Save the dataframe to a csv file
df.to_csv('bert_output.csv', index=False)


from sklearn.feature_extraction.text import CountVectorizer

vectorizer = CountVectorizer(max_features=10, stop_words='english')  

X = vectorizer.fit_transform(df['content'])

# Create a DataFrame with words as columns and documents as rows.
# Each cell represents the count of the word in that specific document.
word_counts_per_doc = pd.DataFrame(X.toarray(), columns=vectorizer.get_feature_names_out())

# Add the cluster labels to this DataFrame
word_counts_per_doc['cluster_label'] = df['cluster_label']

# Group by cluster label and sum across rows
word_counts_per_cluster = word_counts_per_doc.groupby('cluster_label').sum()

# For each cluster, print the words with the highest count
for i in range(10):  # Assuming 10 clusters
    print(f"Most common words in cluster {i}:")
    print(word_counts_per_cluster.loc[i].nlargest(10))


from sklearn.decomposition import PCA
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt

# Assuming embeddings is your array of BERT embeddings

# Apply PCA to reduce the dimensions to 3
pca = PCA(n_components=3)
pca_result = pca.fit_transform(embeddings)

# Create a DataFrame with PCA results
df_pca = pd.DataFrame(data = pca_result, columns = ['pc1', 'pc2', 'pc3'])

# Concatenate the cluster labels to this DataFrame
df_pca['cluster_label'] = df['cluster_label']

# Create a 3D plot
fig = plt.figure(figsize=(10,10))
ax = fig.add_subplot(111, projection='3d')

# For each cluster, plot the points in the cluster in the 3D space
for i in range(10):  # Assuming 10 clusters
    cluster_data = df_pca[df_pca['cluster_label'] == i]
    ax.scatter(cluster_data['pc1'], cluster_data['pc2'], cluster_data['pc3'])

plt.show()
