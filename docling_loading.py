
import os
#from huggingface_hub import snapshot_download
#Run
#snapshot_download(repo_id="ds4sd/docling-models")
#NOT Run
#snapshot_download(repo_id="ds4sd/docling-models",cache_dir="./huggingface_mirrordl")

import pandas as pd
import json
import logging
import time
from pathlib import Path

from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
)
from docling.document_converter import  DocumentConverter, PdfFormatOption

_log =logging.getLogger(__name__)

#artifacts_path = "./huggingface_mirrordl"
#DOCLING_SERVE_ARTIFACTS_PATH = "./huggingface_mirrordl"

def main():
    logging.basicConfig(level=logging.INFO)

    input_doc_path = 'inputdoc/statnett-green-bond-framework-2024.pdf'

    pipeline_options = PdfPipelineOptions()

    pipeline_options.do_ocr = True
    pipeline_options.do_table_structure = True
    pipeline_options.table_structure_options.do_cell_matching = True
    pipeline_options.ocr_options.lang = ["es"]
    #pipeline_options.ocr_options.use_gpu = True

    pipeline_options.accelerator_options = AcceleratorOptions(
        #num_threads=4,device=AcceleratorDevice.CUDA
        num_threads=4, device=AcceleratorDevice.CUDA
    )

    doc_converter = DocumentConverter(
        format_options= {
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )

    start_time = time.time()
    conv_result = doc_converter.convert(input_doc_path)
    end_time = time.time() - start_time

    _log.info(f"Converted {input_doc_path} in {end_time} seconds")

    output_dir = Path("scratch")
    output_dir.mkdir(parents=True, exist_ok=True)
    doc_filename = conv_result.input.file.stem

    with (output_dir / f"{doc_filename}.json").open("w") as fp:
        fp.write(json.dumps(conv_result.document.export_to_dict()))

    with (output_dir / f"{doc_filename}.txt").open("w") as fp:
        fp.write(conv_result.document.export_to_markdown(strict_text=True))

    with (output_dir / f"{doc_filename}.md").open("w") as fp:
        fp.write(conv_result.document.export_to_markdown())

    with (output_dir / f"{doc_filename}.doctags").open("w") as fp:
        fp.write(conv_result.document.export_to_doctags())

    # original
    for table_ix, table in enumerate(conv_result.document.tables):
        table_df: pd.DataFrame = table.export_to_dataframe()
        #print(f"## {table_ix}")
        #print(table_df.to_markdown())

        #table_df.to_excel(output_dir / f"{doc_filename}.xlsx", sheet_name=f"{table_ix}", index=False)

        element_csv_filename = output_dir / f"{doc_filename}-table-{table_ix+1}.csv"
        _log.info(f"Saving CSV to {element_csv_filename}")
        table_df.to_csv(element_csv_filename)

        element_html_filename = output_dir / f"{doc_filename}-table-{table_ix+1}.html"
        _log.info(f"Saving HTML to {element_html_filename}")
        with element_csv_filename.open("w") as fp:
            fp.write(table.export_to_html(doc=conv_result.document))

    # rework

    with pd.ExcelWriter(output_dir / f"{doc_filename}.xlsx") as writer:
        for table_ix, table in enumerate(conv_result.document.tables):
            table_df: pd.DataFrame = table.export_to_dataframe()
            print(f"## {table_ix}")
            print(table_df.to_markdown())
            table_df.to_excel(writer, sheet_name=f"{table_ix}", index=False)

if __name__ == "__main__":
    main()