import os
import sys
import threading

script_dir = "/data/liuzi/Program/AF2/alphafold_non_docker"
database_dir = "/data/liuzi/library/database"


def run_single_AF2(sequence_file, outputdir, gpu_id):

   cmd = "cd " + script_dir + "; bash run_alphafold.sh -d " + database_dir + " -o " + outputdir + " -f " + sequence_file + " -t 2020-05-14 --gpu_device=" + gpu_id
   print(cmd)
   os.system(cmd)

def split_name_list(name_list, num_sublists):

    sublist_length = len(name_list) // num_sublists
    remainder = len(name_list) % num_sublists

    # Create the sublists
    sublists = []
    start = 0
    for i in range(num_sublists):
        if i < remainder:
            end = start + sublist_length + 1
        else:
            end = start + sublist_length
        sublists.append(name_list[start:end])
        start = end

    return sublists

def download_bulk_sequence(sequence_dir, name_list, gpu_id):

    for name in name_list:
        sequence_file = sequence_dir + "/" + name + "/seq.fasta"
        outputdir = sequence_dir + "/" + name + "/"
        run_single_AF2(sequence_file, outputdir, gpu_id)

def read_sequence(sequence_file):  # read sequence

    sequence_dict = dict()

    f = open(sequence_file, "r")
    text = f.read()
    f.close()

    for line in text.splitlines():
        if (line.startswith(">")):
            name = line[1:]
        else:
            sequence_dict[name] = line

    return sequence_dict

def split(sequence_file, outputdir):

    sequence_dict = read_sequence(sequence_file)

    for name in sequence_dict:

        current_dir = outputdir + "/" + name + "/"
        if(os.path.exists(current_dir)==False):
            os.makedirs(current_dir)

        f = open(current_dir + "/seq.fasta", "w")
        f.write(">" + name + "\n")
        f.write(sequence_dict[name] + "\n")
        f.flush()
        f.close()

def download_bulk_sequence_multi_thread(sequence_file, sequence_dir, thread_number, gpu_id): # download protein sequences with information using UniProt ID from a protein list file

    os.system("rm -rf " + sequence_dir)
    split(sequence_file, sequence_dir)
    all_name_list = os.listdir(sequence_dir)
    name_list_array = split_name_list(all_name_list, thread_number)
    threads = []

    for name_list in name_list_array:

        thread = threading.Thread(target=download_bulk_sequence, args=(sequence_dir, name_list, gpu_id, ))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":

    download_bulk_sequence_multi_thread(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4])