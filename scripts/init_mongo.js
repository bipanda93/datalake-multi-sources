db = db.getSiblingDB('datalake');

db.createCollection('products');
db.createCollection('reviews');

db.products.insertOne({
    product_id: "sample",
    product_category_name: "init",
    created_at: new Date()
});

print("MongoDB initialise !");