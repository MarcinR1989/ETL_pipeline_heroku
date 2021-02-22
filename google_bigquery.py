import os
from google.cloud import bigquery
from bs4 import BeautifulSoup
import pandas as pd


def html_to_csv(path):
    # getting the header from HTML file
    list_header = []
    soup = BeautifulSoup(open(path), 'html.parser')
    header = soup.find_all("table")[0].find("tr")
    for items in header:
        try:
            list_header.append(items.get_text())
        except:
            continue

    # getting the data from HTML file
    data = []
    html_data = soup.find_all("table")[0].find_all("tr")[1:]
    for element in html_data:
        sub_data = []
        for sub_element in element:
            try:
                sub_data.append(sub_element.get_text().replace('\n', ''))
            except:
                continue
        data.append(sub_data)

    # Storing the data into Pandas DataFrame
    data_frame = pd.DataFrame(data=data, columns=list_header)
    # Converting Pandas DataFrame into CSV file
    data_frame.to_csv(path.replace('.xls', '.csv'), index=False)


def xls_to_csv(download_dir):
    onlyfiles = [f for f in os.listdir(download_dir) if f.endswith('.xls')]
    for file in onlyfiles:
        df_path = os.path.join(download_dir, file)
        try:
            df = pd.read_excel(df_path)
            dataframe_nowhitespaces = df.apply(
                lambda x: x.str.replace('\n', '')
                if x.dtype == "object" else x
            )
            print(f'Otwarto plik .xls: {file}')
            dataframe_nowhitespaces.to_csv(
                df_path.replace('.xls', '.csv'), index=False, encoding='utf-8'
            )
        except:
            html_to_csv(df_path)
            print(f'Otwarto plik .html: {file}')


def bigquery_connect(download_dir):
    # connections
    client = bigquery.Client()
    onlyfiles = [f for f in os.listdir(download_dir)
                 if f.endswith('.csv')]
    path_platnosc = os.path.join(
        download_dir,
        [elem for elem in onlyfiles
         if elem.startswith("Płatności") or elem.startswith("Payments")][0]
    )
    path_raport_finansow = os.path.join(
        download_dir,
        [elem for elem in onlyfiles if elem.startswith("raport-finansow")][0]
    )
    path_raport_wszystkich_zam = os.path.join(
        download_dir, [elem for elem in onlyfiles
                       if elem.startswith("raport-wszystkich")][0]
    )
    table_id_path = [
        ('charlie.Upload-Platnosci', path_platnosc),
        ('charlie.Upload-Raport-finansow', path_raport_finansow),
        ('charlie.Upload-raport-wszystkich-zam', path_raport_wszystkich_zam)
    ]

    for file in table_id_path:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        with open(file[1], "rb") as source_file:
            job = client.load_table_from_file(source_file, file[0],
                                              job_config=job_config)

        job.result()  # Waits for the job to complete.

        table = client.get_table(file[0])  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), file[0]
            )
        )


if __name__ == '__main__':
    xls_to_csv(os.environ.get("DOWNLOAD_DIR"))
    # html_to_csv()
    bigquery_connect(os.environ.get("DOWNLOAD_DIR"))
