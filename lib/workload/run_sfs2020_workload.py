#!/usr/bin/env python3
import json
import argparse
import subprocess
import datetime
import os
from cephfs_perf_lib import CommonUtils


def main():
    parser = argparse.ArgumentParser(description="Run SPECSTORAGE 2020 workload")
    parser.add_argument(
        "-f", "--config", required=True, help="Path to the SPECSTORAGE config file"
    )
    parser.add_argument(
        "--settings", required=True, help="JSON string containing test settings"
    )

    args = parser.parse_args()

    try:
        settings = json.loads(args.settings)
    except json.JSONDecodeError as e:
        print(f"Error decoding settings JSON: {e}")
        return

    fs_name = settings.get("fs_name", "perf_test_fs")  # fallback if not present
    workload_dir = settings.get("workload_dir")
    results_dir = settings.get("results_dir")

    # Use run_name from settings if provided, otherwise generate it
    run_name = settings.get("run_name")
    if not run_name:
        # Construct a string from mds_settings
        options = CommonUtils.get_workload_base_name(
            "sfs2020", "result", "admin", 0, settings, config=None
        )
        # Remove prefix
        prefix = "sfs2020_result_admin_lp00_"
        if options.startswith(prefix):
            options = options[len(prefix) :]

        # Timestamp
        now = datetime.datetime.now(datetime.timezone.utc)
        timestamp = now.strftime("%Y%m%d-%H%M%S")
        unix_ts = int(now.timestamp())
        full_timestamp = f"{timestamp}-{unix_ts}"

        run_name = f"{full_timestamp}_{options}"

    output_path = args.config

    if workload_dir:
        print(f"Changing directory to {workload_dir}")
        os.chdir(workload_dir)

    if results_dir:
        print(f"Ensuring results directory exists: {results_dir}")
        os.makedirs(results_dir, exist_ok=True)

    cmd = ["python3", os.path.expanduser("./SM2020"), "-r", output_path, "-s", run_name]

    if results_dir:
        cmd.extend(["--results-dir", results_dir])

    print(f"Executing: {' '.join(cmd)}")
    subprocess.run(cmd)

    if results_dir:
        # Inject test parameters into sfssum_<run_name>.xml
        xml_file = os.path.join(results_dir, f"sfssum_{run_name}.xml")
        if os.path.exists(xml_file):
            print(f"Injecting test parameters into {xml_file}...")
            try:
                import xml.etree.ElementTree as ET

                tree = ET.parse(xml_file)
                root = tree.getroot()

                # Check if test_parameters already exists
                test_params = CommonUtils.get_human_readable_settings(settings)

                tree_root = tree.getroot()
                if tree_root.tag == "summary":
                    # We need to create a new root to have summary and test_parameters as siblings
                    new_root = ET.Element("results")
                    new_root.append(tree_root)
                    tree._setroot(new_root)
                    root = new_root
                else:
                    root = tree_root

                params_elem = root.find("test_parameters")

                if params_elem is None:
                    # Inject test parameters
                    params_elem = ET.SubElement(root, "test_parameters")
                    for k, v in test_params.items():
                        param_elem = ET.SubElement(params_elem, "param", name=k)
                        param_elem.text = str(v)

                    # Indent for better readability (Python 3.9+)
                    if hasattr(ET, "indent"):
                        ET.indent(tree, space="  ", level=0)

                    tree.write(xml_file, encoding="utf-8", xml_declaration=True)
                    print(f"Successfully injected test parameters into {xml_file}")
                else:
                    print(
                        f"Test parameters already present in {xml_file}, skipping injection."
                    )

                # Save a JSON version as well
                # We want the original summary root for JSON if we wrapped it
                summary_elem = root.find("summary") if root.tag != "summary" else root
                summary_json = {
                    "id": summary_elem.attrib.get("id"),
                    "runs": [],
                    "test_parameters": test_params,
                }
                for run in summary_elem.findall("run"):
                    run_data = {
                        "time": run.attrib.get("time"),
                        "fingerprint": run.attrib.get("fingerprint"),
                        "version": run.attrib.get("version"),
                        "metrics": {},
                        "benchmark": (
                            run.find("benchmark").attrib.get("name")
                            if run.find("benchmark") is not None
                            else None
                        ),
                        "business_metric": (
                            run.find("business_metric").text
                            if run.find("business_metric") is not None
                            else None
                        ),
                        "valid_run": (
                            run.find("valid_run").text
                            if run.find("valid_run") is not None
                            else True
                        ),
                    }
                    for metric in run.findall("metric"):
                        name = metric.attrib.get("name")
                        units = metric.attrib.get("units")
                        value = metric.text
                        try:
                            f_value = float(value) if value else None
                        except (ValueError, TypeError):
                            f_value = value
                        run_data["metrics"][name] = {"value": f_value, "units": units}
                    summary_json["runs"].append(run_data)

                json_file = xml_file.replace(".xml", ".json")
                with open(json_file, "w") as f:
                    json.dump(summary_json, f, indent=2)
                print(f"Successfully saved JSON summary to {json_file}")
            except Exception as e:
                print(f"Failed to inject test parameters or save JSON: {e}")
        else:
            print(f"XML summary file {xml_file} not found, skipping injection.")


if __name__ == "__main__":
    main()
