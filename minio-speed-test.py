import pandas as pd
import collections
import shutil
import subprocess
import six

from timer import Timer

DISK = 'attached_disk'


def time_copy(src, dst, n=1):
    """
    Copies src to dst n times, returning a list of the copy times in seconds
    """
    times = []
    for i in range(n):
        timer = Timer()
        shutil.copy(src, dst)
        times.append(timer.elapsed())
    return times


def create_file(filename="./tempfile", size="1m"):
    result = subprocess.run(["fallocate", "-l", size, filename], stdout=subprocess.PIPE)
    return filename


def create_files(filename_prefix="tempfile", size="1m", n=1):
    filenames = []
    for i in range(n):
        filenames.append(create_file(filename=f"{filename_prefix}_{size}_{i}", size=size))
    return filenames


def run_case(filenames, size, n, src, dst):
    result = {'size': size, 'n': n, 'src': src, 'dst': dst}
    result['times'] = []
    for filename in filenames:
        result['times'].append(time_copy(f"{src}/{filename}", dst)[0])
    result['avg_time'] = sum(result['times']) / len(result['times'])
    return result


def main():
    output_file = "timing_output.txt"
    cases = [
        {'size': '1k', 'n': 100, 'tenant': 'minimal-tenant1'},
        {'size': '1m', 'n': 100, 'tenant': 'minimal-tenant1'},
        {'size': '10m', 'n': 10, 'tenant': 'minimal-tenant1'},
        {'size': '100m', 'n': 10, 'tenant': 'minimal-tenant1'},
        {'size': '1g', 'n': 5, 'tenant': 'minimal-tenant1'},
        {'size': '1k', 'n': 100, 'tenant': 'premium-tenant1'},
        {'size': '1m', 'n': 100, 'tenant': 'premium-tenant1'},
        {'size': '10m', 'n': 10, 'tenant': 'premium-tenant1'},
        {'size': '100m', 'n': 10, 'tenant': 'premium-tenant1'},
        {'size': '1g', 'n': 5, 'tenant': 'premium-tenant1'},
    ]

    results = []

    for case in cases:
        print(case)
        size = case['size']
        n = case['n']
        tenant = case['tenant']

        local_source_dir = "./source/"
        local_target_dir = "./target/"
        minio_target_dir1 = f"~/minio/{tenant}/private/minio_speed_test/target1/"
        minio_target_dir2 = f"~/minio/{tenant}/private/minio_speed_test/target2/"
        # minio_target_dir1 = f"./target1/"
        # minio_target_dir2 = f"./target2/"
        all_directories = [local_source_dir, local_target_dir, minio_target_dir1, minio_target_dir2]

        # Create all directories and local files in attached storage
        for d in all_directories:
            subprocess.run(["mkdir", "-p", d])
        filepaths = create_files(filename_prefix=f"{local_source_dir}/tempfile", size=size, n=n)
        filenames = [p.split('/')[-1] for p in filepaths]

        # Copy from attached to attached
        results.append(run_case(filenames, size, n, local_source_dir, local_target_dir))

        # Copy from attached to minio
        results.append(run_case(filenames, size, n, local_source_dir, minio_target_dir1))

        # Copy from minio to attached
        results.append(run_case(filenames, size, n, minio_target_dir1, local_target_dir))

        # Copy from minio to minio
        results.append(run_case(filenames, size, n, minio_target_dir1, minio_target_dir2))

        # Cleanup files
        for d in all_directories:
            for f in filenames:
                subprocess.run(["rm", f"{d}{f}"])

        # Write current results (so we can easily monitor progress - a little wasteful but whatever...)
        current_results = pd.DataFrame(results)
        current_results.to_csv(output_file)


if __name__ == "__main__":
    main()


# Helpers
def is_iterable(arg):
    """
    Returns whether an argument is an iterable but not a string

    From stackoverflow: "how to tell a varaiable is iterable but not a string"

    Args:
        arg: some variable to be tested

    Returns:
        (bool)
    """
    return (
            isinstance(arg, collections.Iterable)
            and not isinstance(arg, six.string_types)
    )


def make_iterable(arg):
    """
    Returns arg as an iterable.

    If arg is already iterable, return as is.  Otherwise, return [arg]

    Args:
        arg: some variable to be returned as an iterable

    Returns:
        (iterable): Either arg as is, or [arg]
    """
    if is_iterable(arg):
        return arg
    else:
        return [arg]