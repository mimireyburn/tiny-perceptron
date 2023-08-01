    labels = ["mushroom", "airplane", "basketball ball", "banana", "tennis racket", "cat", "ship", "bird", "truck", "guitar", "brocolli", "horse", "fish", "chair", "dog", "grapes", "automobile", "robot", "corn", "rugby ball", "bicycle", "apple", "soccer ball", 'mushroom', 'airplane', 'basketball ball', 'banana', 'tennis racket', 'cat', 'ship', 'bird', 'truck', 'guitar', 'brocolli', 'horse', 'fish', 'chair', 'dog', 'grapes', 'automobile', 'robot', 'corn', 'rugby ball', 'bicycle', 'apple', 'soccer ball']
    for label in labels:
        app.state.a.delete(str(label))
    all_items = app.state.a.keys("item:*")
    for item in all_items:
        item_labels = app.state.a.hget(item, "labels")
        item_labels = item_labels[1:-1].split(",")
        item_id = app.state.a.hget(item, "id")
        for label in item_labels:
            app.state.a.sadd(label[1:-1].replace('"', ''), str(item_id))