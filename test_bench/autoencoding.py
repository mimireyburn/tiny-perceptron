import torch
import torch.nn as nn
import torch.optim as optim
import pandas as pd
from torch.utils.data import DataLoader
from torch.utils.data import Dataset
from torchvision import transforms
from PIL import Image
import requests
from io import BytesIO

counter = 0
class URLDataset(Dataset):
    def __init__(self, url_df, transform=None):
        self.url_df = url_df
        self.transform = transform

    def __len__(self):
        return len(self.url_df)

    def __getitem__(self, idx):
        url = self.url_df.iloc[idx, 0]
        response = requests.get(url)
        image = Image.open(BytesIO(response.content)).convert('RGB')
        if self.transform:
            image = self.transform(image)
        return image
    
# Define the Convolutional Encoder model
class ConvEncoder(nn.Module):
    def __init__(self):
        super(ConvEncoder, self).__init__()

        self.conv1 = nn.Conv2d(3, 16, kernel_size=3, stride=2, padding=1)
        self.conv2 = nn.Conv2d(16, 32, kernel_size=3, stride=2, padding=1)
        self.conv3 = nn.Conv2d(32, 64, kernel_size=3, stride=2, padding=1)
        self.conv4 = nn.Conv2d(64, 128, kernel_size=3, stride=2, padding=1)
        self.conv5 = nn.Conv2d(128, 256, kernel_size=3, stride=2, padding=1)
        self.relu = nn.ReLU(inplace=True)

    def forward(self, x):
        x = self.relu(self.conv1(x))
        x = self.relu(self.conv2(x))
        x = self.relu(self.conv3(x))
        x = self.relu(self.conv4(x))
        x = self.relu(self.conv5(x))
        return x

# Define the Convolutional Decoder model
class ConvDecoder(nn.Module):
    def __init__(self):
        super(ConvDecoder, self).__init__()

        self.deconv1 = nn.ConvTranspose2d(256, 128, kernel_size=4, stride=2, padding=1)
        self.deconv2 = nn.ConvTranspose2d(128, 64, kernel_size=4, stride=2, padding=1)
        self.deconv3 = nn.ConvTranspose2d(64, 32, kernel_size=4, stride=2, padding=1)
        self.deconv4 = nn.ConvTranspose2d(32, 16, kernel_size=4, stride=2, padding=1)
        self.deconv5 = nn.ConvTranspose2d(16, 3, kernel_size=4, stride=2, padding=1)
        self.relu = nn.ReLU(inplace=True)

    def forward(self, x):
        x = self.relu(self.deconv1(x))
        x = self.relu(self.deconv2(x))
        x = self.relu(self.deconv3(x))
        x = self.relu(self.deconv4(x))
        x = self.relu(self.deconv5(x))
        return x

# Define the autoencoder model using ConvEncoder and ConvDecoder
class Autoencoder(nn.Module):
    def __init__(self):
        super(Autoencoder, self).__init__()
        self.encoder = ConvEncoder()
        self.decoder = ConvDecoder()

    def forward(self, x):
        x = self.encoder(x)
        x = self.decoder(x)
        return x

# Load the CSV file with image URLs
url_df = pd.read_csv('/root/tiny-perceptron/test_bench/image_data_urls.csv')

# Define transformations
transforms = transforms.Compose([transforms.ToTensor()])

# Create dataset with URLs
url_dataset = URLDataset(url_df=url_df, transform=transforms)

# Create DataLoader for the URL dataset
url_dataloader = DataLoader(url_dataset, batch_size=32, shuffle=True)

# Create the autoencoder model
autoencoder = Autoencoder()

# Use GPU if available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
autoencoder.to(device)

# Define the loss function and optimizer
criterion = nn.MSELoss()
optimizer = optim.Adam(autoencoder.parameters(), lr=1e-3)

# Training loop
EPOCHS = 10
for epoch in range(EPOCHS):
    for batch_img in url_dataloader:
        counter+=1
        print(counter)
        batch_img = batch_img.to(device)
        optimizer.zero_grad()
        output = autoencoder(batch_img)
        loss = criterion(output, batch_img)
        loss.backward()
        optimizer.step()
    print(f"Epoch {epoch+1}/{EPOCHS}, Loss: {loss.item()}")

# Save the trained autoencoder model
torch.save(autoencoder.state_dict(), "autoencoder_model.pt")