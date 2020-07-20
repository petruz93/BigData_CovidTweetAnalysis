from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('products')

rows = session.execute('SELECT * FROM products.bycicle')

for r in rows:
    print(r)
