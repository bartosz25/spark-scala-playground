import argparse

file_1_gb_bytes = 1000000000
file_500_mb_bytes = file_1_gb_bytes/2
file_3_gb_bytes = file_1_gb_bytes*2


def generate_file(file_size, file_name):
	mb_in_file = 100
	bytes_in_file = 1000*1000*mb_in_file
	with open(file_name, 'w') as generated_file:
		file_rounds = file_size/bytes_in_file
		index = 0
		for number in range(0, int(file_rounds)):
			generated_file.write(str(index) * bytes_in_file)
			generated_file.write('\n')
			index += 1

print('Generating 3GB file')
generate_file(file_3_gb_bytes, 'generated_file_3_gb.txt')
print('Generating 1GB file')
generate_file(file_1_gb_bytes, 'generated_file_1_gb.txt')
print('Generating 500MB file')
generate_file(file_500_mb_bytes, 'generated_file_500_mb.txt')
