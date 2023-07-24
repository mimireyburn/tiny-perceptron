import requests
import psycopg2
from psycopg2 import sql
import torch

from torchvision import models

def label_img_items():
    connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS item_img_content (
        item_key UUID PRIMARY KEY,
        bucket_key VARCHAR NOT NULL,
        type VARCHAR(5) NOT NULL,
        url TEXT NOT NULL,
        labels_resnet50 TEXT,
        labels_resnet34 TEXT,
        labels_resnet101 TEXT,
        labels_resnet152 TEXT
    )
"""
    ) 
    connection.commit()

    # Insert the data into the items table
    cursor.execute(
        """
        SELECT item_key, bucket_key
        FROM public.items
        WHERE type = 'img'
        """
    ) 
    rows = cursor.fetchall()

    domain= "s3.us-east-1.amazonaws.com"

    from torchvision import models, transforms
    from PIL import Image
    import io

    # Load a pretrained ResNet models
    model_list = [models.resnet34(pretrained=True), models.resnet101(pretrained=True), models.resnet152(pretrained=True)]
    model_names = ['resnet34', 'resnet101', 'resnet152']
    import requests
    import json

    LABELS_URL = "https://raw.githubusercontent.com/anishathalye/imagenet-simple-labels/master/imagenet-simple-labels.json"
    response = requests.get(LABELS_URL)
    labels = json.loads(response.text)

    with open('imagenet_classes.txt', 'w') as f:
        for label in labels:
            f.write(f"{label}\n")
    # ImageNet classes
    with open('imagenet_classes.txt') as f:
        labels = [line.strip() for line in f.readlines()]

    for model, model_name in zip(model_list, model_names):
        model.eval()
        def get_img_item_label(bucket_key, domain, item_key):
            url = f"https://{bucket_key}.{domain}/{item_key}.jpg"
            response = requests.get(url)
            if response.status_code == 200:
                # Load and preprocess the image
                image = Image.open(io.BytesIO(response.content))
                preprocess = transforms.Compose([
                    transforms.Resize(256),
                    transforms.CenterCrop(224),
                    transforms.ToTensor(),
                    transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
                ])
                input_tensor = preprocess(image)
                input_batch = input_tensor.unsqueeze(0)  # create a mini-batch as expected by the model

                # Forward pass
                with torch.no_grad():
                    output = model(input_batch)

                # Get the label
                _, predicted_idx = torch.max(output, 1)
                label = labels[predicted_idx.item()]

                return label

            else:
                return None

        for row in rows:
            item_key, bucket_key = row
            # Send a GET request to the item's URL
            label = get_img_item_label(bucket_key,domain,item_key)

            # Update the new column in the table
            update = sql.SQL(f"""
                UPDATE item_img_content 
                SET labels_{model_name} = %s 
                WHERE item_key = %s
            """)
            cursor.execute(update, (label, item_key))

    connection.commit()
    cursor.close()
    connection.close()

label_img_items()


