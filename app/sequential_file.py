from app.binary_file import BinaryFile
from app.record import Record
from typing import Dict
from os import ftruncate

class SequentialFile(BinaryFile):
    def __init__(
            self, 
            filename: str, 
            record: Record, 
            blocking_factor: int, 
            empty_record: Dict, 
            empty_key: int = -1
            ):
        BinaryFile.__init__(self, filename, record, blocking_factor, empty_record, empty_key)

    def insert_record(self, rec: Dict):
        with open(self.filename, 'rb+') as file:
            check, block_index, rec_index = self.find_by_id(rec["id"])
            if check == None:
                file.seek(block_index)
                block = self.read_block(file)
                if block[rec_index]["id"] == self.empty_key:
                    block[rec_index] = rec
                    file.seek(-self.block_size, 1)
                    self.write_block(file, block)
                else:
                    moved = block[-1]
                    block = block[:rec_index] + [rec] + block[rec_index:-1]
                    file.seek(-self.block_size, 1)
                    self.write_block(file, block)
                    while moved["id"] != self.empty_key and block != []:
                        block = self.read_block(file)
                        file.seek(-self.block_size, 1)
                        nextblock = [moved] + block[:-1]
                        self.write_block(file, nextblock)
                        moved = block[-1]
                file.seek(-self.block_size, 2)
                last = self.read_block(file)
                if last[-1]["id"] != self.empty_key: # proveravamo da li smo popunili poslednji blok
                    self.write_emptyblock(file)

    def find_by_id(self, id: str):
        with open(self.filename, 'rb') as file:
            while True:
                block = self.read_block(file)
                for i in range(self.blocking_factor):
                    if block[i]["id"] == id:
                        return block[i], file.tell() - self.block_size, i
                    elif block[i]["id"] == self.empty_key or block[i]["id"] > id:
                        return None, file.tell() - self.block_size, i

    def delete_record(self, id: str):
        with open(self.filename, 'rb+') as file:
            _, block_index, rec_index = self.find_by_id(id)
            if block_index != None:
                file.seek(block_index, 0)
                block = self.read_block(file)
                nextblock = self.read_block(file)
                while nextblock != []:
                    if rec_index == self.blocking_factor - 1:
                        block[rec_index] = nextblock[0]
                        file.seek(-self.block_size * 2, 1)
                        self.write_block(file, block)
                        block = self.read_block(file)
                        nextblock = self.read_block(file)
                        rec_index = 0
                    else:
                        block[rec_index] = block[rec_index + 1]
                        rec_index += 1
                if block[0]["id"] == self.empty_key: # prepisali smo EOF na kraj fajla - ovaj blok brišemo
                    file.seek(-self.block_size, 2)
                    ftruncate(file.fileno(), file.tell())
                else:
                    file.seek(-self.block_size, 2)
                    self.write_block(file, block[1:] + [self.empty_record])

    def update_record(self, newrec: Dict):
        with open(self.filename, 'rb+') as file:
            _, block_index, rec_index = self.find_by_id(newrec["id"])
            if block_index != None:
                file.seek(block_index, 0)
                block = self.read_block(file)
                block[rec_index] = newrec
                file.seek(-self.block_size, 1)
                self.write_block(file, block)