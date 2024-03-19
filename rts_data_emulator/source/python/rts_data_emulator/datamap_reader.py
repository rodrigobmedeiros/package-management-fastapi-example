import pandas as pd
import numpy as np
from constants import RJS739_DATA_PATH, RJS742_DATA_PATH, DIRECTORY_LEVELS_ABOVE
from multiprocessing import Pool
from pathlib import Path
from utils import get_data_directory
import multiprocessing
import pickle
import os

def save_data_to_pickle(data, file_name):
    with open(file_name, "wb") as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)


def load_data_from_pickle(file_name):
    with open(file_name, "rb") as f:
        return pickle.load(f)


def get_files_from_folder(folder: Path, extension: str) -> list[Path]:
    return list(folder.glob(f"*.{extension}"))


def read_csv_data_from_file(file_name: Path) -> pd.DataFrame:
    df = pd.read_csv(
        file_name,
        delimiter=";",
        header=0,
        skiprows=4,
        names=["date_time", "reading", "extra"],
    )
    df = df.drop(columns=["extra"])
    df["reading"] = df["reading"].str.replace(",", ".")
    # df["reading"].replace({"On": 1, "Off": 0}, inplace=True)
    df["date_time"] = pd.to_datetime(df["date_time"], format="%d/%m/%Y %H:%M:%S")

    df["measurement"] = pd.to_numeric(df["reading"], errors="coerce")
    df["valid"] = df["measurement"].notna()

    return df


def save_df_to_pickle(df: pd.DataFrame, file_name: Path):
    df.to_pickle(file_name)


def create_tag_dict_from_df(df: pd.DataFrame) -> dict:
    tag_dict = {}
    for _, row in df.iterrows():
        if (
            (row["PI Tag (main)"] != "")
            and (row["PI Tag (main)"] is not None)
            and (row["PI Tag (main)"] != np.NaN)
            and (row["PI Tag (main)"] != "none")
        ):
            tag_dict[row["PI Tag (main)"]] = {
                "description": row["Tag Description"],
                "type": row["Type"],
                "units": row["Units"],
                "backup": False,
                "main": row["PI Tag (main)"],
            }
            if row["PI Tag (backup)"] != "":
                tag_dict[row["PI Tag (backup)"]] = {
                    "description": row["Tag Description"],
                    "type": row["Type"],
                    "units": row["Units"],
                    "backup": True,
                    "main": row["PI Tag (main)"],
                }

    return tag_dict


def read_csv_save_pickle(csv_file_path: Path) -> dict[str, str]:
    filename, _ = csv_file_path.name.split(".")
    parent_folder = csv_file_path.parent
    pickle_file = filename + ".pkl"
    pickle_file_path = parent_folder / pickle_file
    if pickle_file_path.exists():
        print(f"pickle file {pickle_file} already exists, skipping...")
        return {"csv_path": csv_file_path.name, "pickle_path": pickle_file}

    print(f"running read_csv_save_pickle for file {filename}...")
    csv = read_csv_data_from_file(csv_file_path)
    save_df_to_pickle(csv, pickle_file_path)

    return {"csv_path": csv_file_path.name, "pickle_path": pickle_file}


def tag_dict_builder_from_datamap_files() -> tuple[dict, dict]:

    current_folder = Path().resolve()
    data_folder = get_data_directory(current_folder, DIRECTORY_LEVELS_ABOVE)

    # Read the data from the file
    df_injector = pd.read_csv(
        data_folder / "datamap" / "datamap_injector.csv",
        delimiter=";",
        header=0,
        names=["Tag Description", "Type", "Units", "PI Tag (main)", "PI Tag (backup)"],
    )
    df_producer = pd.read_csv(
        data_folder / "datamap" / "datamap_producer.csv",
        delimiter=";",
        header=0,
        names=["Tag Description", "Type", "Units", "PI Tag (main)", "PI Tag (backup)"],
    )

    # Print the data
    print("= Injector ================================================================")
    print(df_injector.head())
    print("= Producer ================================================================")
    print(df_producer.head())
    print("===========================================================================")

    # print('Creating tag dictionary for injector...')
    tag_dict_injector = create_tag_dict_from_df(df_injector)
    # print('Creating tag dictionary for producer...')
    tag_dict_producer = create_tag_dict_from_df(df_producer)

    return tag_dict_injector, tag_dict_producer

def read_df_pickle(file_name: Path) -> pd.DataFrame:
    return pd.read_pickle(file_name)

def read_pickle_file_tags(pickle_file: Path) -> dict[str, int]:
    print(f"running read_pickle_file_tags for file {pickle_file.name}...\n")
    df = pd.read_pickle(pickle_file)
    count_num = len(df[df.valid])
    df = df[~df.valid]
    df.reading.to_dict()

    ret_dict = df.reading.value_counts().to_dict()
    ret_dict["numeric"] = count_num

    return ret_dict


def get_all_tags_from_files(file_list: list[Path], remove_file_extension: bool = True) -> dict[str, dict[str, int]]:
    use_cpus = multiprocessing.cpu_count() - 1
    if use_cpus < 1:
        use_cpus = 1

    results = {}

    with Pool(use_cpus) as p:
        results_list = p.map(read_pickle_file_tags, file_list)
    for index, m_file in enumerate(file_list):
        if remove_file_extension:
            results[os.path.splitext(m_file.name)[0]] = results_list[index]
        else:
            results[m_file.name] = results_list[index]

    return results


def convert_csv_to_pickle() -> dict:
    current_folder = Path().resolve()
    data_folder = get_data_directory(current_folder, DIRECTORY_LEVELS_ABOVE)

    use_cpus = multiprocessing.cpu_count() - 1
    if use_cpus < 1:
        use_cpus = 1

    producer_folder = data_folder / RJS739_DATA_PATH
    injector_folder = data_folder / RJS742_DATA_PATH

    producer_files = get_files_from_folder(producer_folder, "csv")
    injector_files = get_files_from_folder(injector_folder, "csv")

    results = {}
    print("Producer files:")
    with Pool(use_cpus) as p:
        results["producer"] = p.map(read_csv_save_pickle, producer_files)

    print("Injector files:")
    with Pool(use_cpus) as p:
        results["injector"] = p.map(read_csv_save_pickle, injector_files)

    return results
