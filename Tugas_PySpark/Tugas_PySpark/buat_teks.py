with open("dataset_5000.txt", "w", encoding="utf-8") as f:
    for i in range(5500):
        f.write(f"Ini adalah baris ke {i}. Belajar apache spark untuk pemrosesan data paralel. Spark membuat data besar menjadi mudah diproses.\n")
print("File dataset_5000.txt berhasil diisi 5.500 baris!")