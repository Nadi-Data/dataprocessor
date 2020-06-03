class Readcsv():
    
    def __init__(self, file, read_mode=None, chunk_size=None, row_sep=None):
        self.file = file
        self.read_mode = read_mode
        self.chunk_size = chunk_size
        self.row_sep = row_sep

    def read_in_chunks(self, f):
        """Read file in chunks for processing"""
        curr_row = ''
        while True:
            chunk = f.read(self.chunk_size)
            if chunk == '': # End of the file
                yield curr_row
                break
            while True:
                i = chunk.find(self.row_sep)
                if i == -1:
                    break
                yield curr_row + chunk[:i]
                curr_row = ''
                chunk = chunk[i+1:]
            curr_row += chunk


    def process_chunks(self):
        with open(self.file, self.read_mode) as f:
            lines = []
            row_count = 0
            """ Process each data chunk retrieved from file"""
            for piece in self.read_in_chunks(f):
                if piece == '':
                    break
                line = piece.split(',')
                """ Convert Header into columns"""
                if row_count == 0:
                    header = line
                    row_count += 1
                    continue
                else:
                    """ Prepare column , value pairs in form of dictionary"""
                    line = dict(zip(header,line))
                    lines.append(line)
                    yield lines
                    lines.pop()
                    row_count += 1


