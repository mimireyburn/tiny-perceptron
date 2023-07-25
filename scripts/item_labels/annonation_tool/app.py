from flask import Flask, render_template, request, make_response
import psycopg2
import csv
import io

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    connection = psycopg2.connect(host="65.109.54.241", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
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

    # Count the remaining rows to label
    cur.execute("SELECT COUNT(*) FROM item_img_content WHERE user_input1 IS NULL")
    remaining_rows = cur.fetchone()[0]

    # After updating (if it was a POST request), or if it's a GET request, get a new image
    cur.execute("SELECT item_key, url, labels_resnet50, labels_resnet34, labels_resnet101, labels_resnet152 FROM item_img_content WHERE user_input1 IS NULL ORDER BY random() LIMIT 1")
    row = cur.fetchone()

    if row is None:
        return "No more images to label."

    item_id, url, resnet50, resnet34, resnet101, resnet152 = row
    labels = set([resnet50, resnet34, resnet101, resnet152])  # Create a set to remove duplicate labels
    

    return render_template('label.html', url=url, labels=labels, item_id=item_id, remaining_rows=remaining_rows)  # Pass the item_id to the template as well

@app.route('/download', methods=['GET'])
def download():
    connection = psycopg2.connect(host="65.109.54.241", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    cur = connection.cursor()

    # Fetch all records
    cur.execute("SELECT item_key, bucket_key, type, url, user_input1 FROM item_img_content where user_input1 is not null")
    records = cur.fetchall()

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['item_key', 'bucket_key', 'type', 'url', 'user_input1'])  # add your column headers here
    cw.writerows(records)

    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    output.headers["Content-type"] = "text/csv"

    return output

@app.route('/suggestions', methods=['GET'])
def suggestions():
    term = request.args.get('term')  # Get the term from the query string

    connection = psycopg2.connect(host="65.109.54.241", user="root", port=5432, database="W9sV6cL2dX", password="E5rG7tY3fH")
    cur = connection.cursor()

    # Fetch suggestions from the database
    cur.execute("SELECT DISTINCT user_input1 FROM item_img_content WHERE user_input1 ILIKE %s LIMIT 10", (term + '%',))
    records = cur.fetchall()

    # Return the suggestions as JSON
    return jsonify([row[0] for row in records])


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000 ,debug=True)
