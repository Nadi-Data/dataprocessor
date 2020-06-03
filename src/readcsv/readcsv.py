def read_in_chunks(file, chunk_size, row_sep):
    """Read file in chunks for processing"""
    curr_row = ''
    while True:
        chunk = file.read(chunk_size)
        if chunk == '': # End of the file
            yield curr_row
            break
        while True:
            i = chunk.find(row_sep)
            if i == -1:
                break
            yield curr_row + chunk[:i]
            curr_row = ''
            chunk = chunk[i+1:]
        curr_row += chunk


def process_chunks(file, mode, chunk_size, row_sep):
    with open(file,mode) as f:
        lines = []
        row_count = 0
        for piece in read_in_chunks(f, chunk_size, row_sep):
            if piece == '':
                break
            line = piece.split(',')
            if row_count == 0: # header
                header = line
                row_count += 1
                continue
            else:
                line = dict(zip(header,line))
                lines.append(line)
                yield lines
                lines.pop()
                row_count += 1

for line in process_chunks('/Users/sriyan/Downloads/sales.csv','r',1024,'\n'):
    print(line)