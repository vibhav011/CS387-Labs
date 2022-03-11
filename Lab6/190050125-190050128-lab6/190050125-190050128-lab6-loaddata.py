import csv

def main_function(filename):
    NODES = [('users','User','name'), ('tweets','Tweet','text'), ('hashtags','Hashtag','tag')]
    RELS = [('follows', 'Follows', 'User', 'User'), ('sent', 'Sent', 'User', 'Tweet'), ('mentions', 'Mentions', 'Tweet', 'User'), ('contains', 'Contains', 'Tweet', 'Hashtag')]

    query_string = ''

    for k in NODES:
        with open('lab6-data/' + k[0] + '.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            data = [tuple(map(lambda x : str(x).replace('"', '\\"'), row)) for row in reader]
            query = ','.join(list(map(lambda t: f'({k[1]}_{t[0]}:{k[1]}{{{k[2]}: "{t[1]}"}})', data)))
            query_string += 'CREATE ' + query + '\n'
    
    for k in RELS:
        with open('lab6-data/' + k[0] + '.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            data = [tuple(map(lambda x : str(x), row)) for row in reader]
            query = ','.join(list(map(lambda t: f'({k[2]}_{t[0]})-[:{k[1]}]->({k[3]}_{t[1]})', data)))
            query_string += 'CREATE ' + query + '\n'
    
    with open(filename, 'w') as f:
        f.write(query_string)

if __name__ == "__main__":
    main_function('lab6-data.cypher')