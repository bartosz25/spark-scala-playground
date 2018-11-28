import argparse

file_1_gb_bytes = 1000000000
file_500_mb_bytes = file_1_gb_bytes/2
file_3_gb_bytes = file_1_gb_bytes*3


def generate_file(file_size, file_name, mb_in_line): 
	bytes_in_line = 1000*1000*mb_in_line
	with open(file_name, 'w') as generated_file:
		file_rounds = file_size/bytes_in_line
		print('Writing {lines} lines'.format(lines=file_rounds))
		index = 0
		for number in range(0, int(file_rounds)):
			generated_file.write(str(index) * bytes_in_line)
			generated_file.write('\n')
			index += 1
			if index == 10:
				index = 0
			
print('Generating 3GB file - 100mb/line')
generate_file(file_3_gb_bytes, 'generated_file_3_gb.txt', 100)
print('Generating 1GB file - 100mb/line')
generate_file(file_1_gb_bytes, 'generated_file_1_gb.txt', 100)
print('Generating 500MB file - 100mb/line')
generate_file(file_500_mb_bytes, 'generated_file_500_mb.txt', 100)
print('Generating 1GB file - 1mb/line')
generate_file(file_1_gb_bytes, 'generated_file_1_gb_1mb.txt', 1)
