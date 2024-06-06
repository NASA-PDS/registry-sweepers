import logging
import os

logging.basicConfig(level=logging.INFO)

base_dir = os.path.expanduser('~/Documents/opensearch-diff-dumps')

indices = [os.path.splitext(fn)[0] for fn in os.listdir(base_dir) if os.path.splitext(fn)[1] == '.aossdump']
for index in indices:
    logging.info(f'Diffing {index}')
    try:
        basis_filepath = os.path.join(base_dir, f'{index}.esdump')
        diffed_filepath = os.path.join(base_dir, f'{index}.aossdump')
        output_filepath = os.path.join(base_dir, f'{index}.diff')

        diff_count = 0

        with open(basis_filepath) as basis_f, \
            open(diffed_filepath) as diffed_f, \
            open(output_filepath, 'w+') as out_f:
            basis_line = basis_f.readline()
            diffed_line = diffed_f.readline()

            while basis_line != '' and diffed_line != '':
                if basis_line < diffed_line:  # if diffed file is missing the lidvid from basis file
                    out_f.writelines([basis_line])
                    basis_line = basis_f.readline()
                    diff_count += 1
                elif diffed_line < basis_line:  # if diffed file lidvid is missing from basis file
                    logging.warning(f'LIDVID is present in aoss file but not in es file: {diffed_line.strip()}')
                    diffed_line = diffed_f.readline()
                else: # if lidvid is in both files
                    basis_line = basis_f.readline()
                    diffed_line = diffed_f.readline()

            while basis_line != '':
                out_f.writelines([basis_line])
                basis_line = basis_f.readline()
                diff_count += 1

        logging.info(f'Identified {diff_count} missing lidvids in {index}')

        if diff_count == 0:
            logging.info(f'No missing lidvids - deleting {output_filepath}')
            os.remove(output_filepath)
    except FileNotFoundError as err:
        print(err)
