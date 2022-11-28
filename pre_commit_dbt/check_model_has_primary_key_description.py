import argparse
from typing import Any
from typing import Dict, List
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import JsonOpenError
from pre_commit_dbt.utils import get_missing_file_paths
from pre_commit_dbt.utils import get_model_sqls
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import yellow


def check_primary_key_description(
        paths: Sequence[str], manifest: Dict[str, Any]
) -> int:
    paths = get_missing_file_paths(paths, manifest)

    status_code = 0
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())
    missing: List[str] = []

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)
    for model in models:
        models_missing_pkey = {
            model.model_name
            for key, value in model.node.get("columns", {}).items()
            if value['tags'] and 'primary-key' in value['tags'] and
            (not value['description'] or len(value['description']) < 2)  # checking for non-whitespace description value
        }
        if models_missing_pkey:
            missing.extend(models_missing_pkey)

    if missing:
        status_code = 1
        result = "\n- ".join(list(missing))
        print(
            f"Following models are missing primary-key description:\n- {yellow(result)}",
        )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)

    args = parser.parse_args(argv)

    # Todo: Comment out this block to test.
    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    test_manifest = {
        "nodes": {
            "model.finnauto.dim_product": {
                "name": "dim_product",
                "path": "aa/bb/dim_product.sql",
                "root_path": "/path/to/aa",
                "columns": {
                    "row_nr": {
                        "name": "row_nr",
                        "description": "",
                        "meta": {},
                        "tags": []},
                    "product_id": {
                        "name": "product_id",
                        "description": "",
                        "meta": {},
                        "tags": []},
                    "dim_pkey_product": {
                        "name": "dim_pkey_product",
                        "description": "test model",
                        "meta": {},
                        "tags": ["primary-key"]}
                }
            },
            "model.finnauto.dim_product_fail": {
                "name": "dim_product_fail",
                "path": "aa/bb/dim_product_fail.sql",
                "root_path": "/path/to/aa",
                "columns": {
                    "row_nr": {
                        "name": "row_nr",
                        "description": "",
                        "meta": {},
                        "tags": []},
                    "product_id": {
                        "name": "product_fail_id",
                        "description": "",
                        "meta": {},
                        "tags": []},
                    "dim_pkey_product": {
                        "name": "dim_pkey_product_fail",
                        "description": " ",
                        "meta": {},
                        "tags": ["primary-key"]}
                }
            },
            "model.finnauto.dim_product_fail_2": {
                "name": "dim_product_fail_2",
                "path": "aa/bb/dim_product_fail_2.sql",
                "root_path": "/path/to/aa",
                "columns": {
                    "row_nr": {
                        "name": "row_nr",
                        "description": "",
                        "meta": {},
                        "tags": []},
                    "product_id": {
                        "name": "product_fail_2_id",
                        "description": "",
                        "meta": {},
                        "tags": []},
                    "dim_pkey_product": {
                        "name": "dim_pkey_product_fail_2",
                        "description": "",
                        "meta": {},
                        "tags": ["primary-key"]}
                }
            }
        }
    }
    test_filenames = ["aa/bb/dim_product.sql", "aa/bb/dim_product_fail.sql", "aa/bb/dim_product_fail_2.sql"]

    # Use this call to test
    # return check_primary_key_description(paths=test_filenames, manifest=test_manifest)

    return check_primary_key_description(paths=args.filenames, manifest=manifest)


if __name__ == "__main__":
    exit(main())
