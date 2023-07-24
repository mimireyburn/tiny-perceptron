from flask import Flask, render_template, request
import psycopg2


app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    connection = psycopg2.connect(host="localhost", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    cur = connection.cursor()

    if request.method == 'POST':
        item_id = request.form.get('item_id')  # Get the item_id from the form
        selected_labels = request.form.getlist('label')  # Get all selected labels
        custom_label = request.form.get('custom_label')  # Get the custom label

        # If 'None of these' was selected, do not update user_input1, instead just pass
        if 'None of these' in selected_labels:
            selected_label = 'NoneOfThese'
        else:
            if custom_label:  # If custom_label is not empty, add it to selected_labels
                selected_labels.append(custom_label)

            selected_label = ", ".join(selected_labels)  # Convert the list to a string

        # Update the user_input1 column with the selected labels
        cur.execute("UPDATE item_img_content SET user_input1 = %s WHERE item_key = %s", (selected_label, item_id))
        connection.commit()

    # After updating (if it was a POST request), or if it's a GET request, get a new image
    cur.execute("SELECT item_key, url, labels_resnet50, labels_resnet34, labels_resnet101, labels_resnet152 FROM item_img_content WHERE user_input1 IS NULL LIMIT 1")
    row = cur.fetchone()

    if row is None:
        return "No more images to label."

    item_id, url, resnet50, resnet34, resnet101, resnet152 = row
    labels = set([resnet50, resnet34, resnet101, resnet152])  # Create a set to remove duplicate labels
    labels.add('None of these')

    return render_template('label.html', url=url, labels=labels, item_id=item_id)  # Pass the item_id to the template as well

if __name__ == "__main__":
    app.run(debug=True)
