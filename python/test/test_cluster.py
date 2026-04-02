import json
import os
import pytest
from ai.chronon.repo.cluster import (
    generate_dataproc_cluster_config,
    generate_emr_cluster_config,
    fixed_gcp_cluster,
    get_gcp_init_scripts,
)


class TestDataprocClusterConfig:
    """Tests for Dataproc cluster configuration generation."""

    def test_basic_dataproc_config(self):
        """Verify basic Dataproc cluster configuration structure."""
        config_json = generate_dataproc_cluster_config(
            num_workers=10,
            project_id="test-project",
            artifact_prefix="gs://test-bucket/artifacts",
            version="1.0.0",
        )
        config = json.loads(config_json)

        assert "gceClusterConfig" in config
        assert "masterConfig" in config
        assert "workerConfig" in config
        assert "softwareConfig" in config
        assert "initializationActions" in config
        assert config["workerConfig"]["numInstances"] == 10
        assert config["masterConfig"]["numInstances"] == 1

    def test_initialization_scripts_are_included(self):
        """Verify default GCP initialization scripts are included in the config."""
        config_json = generate_dataproc_cluster_config(
            num_workers=5,
            project_id="test-project",
            artifact_prefix="gs://test-bucket/artifacts",
            version="1.2.3",
        )
        config = json.loads(config_json)

        init_actions = config["initializationActions"]
        executable_files = [action["executable_file"] for action in init_actions]

        # Verify all default scripts are present
        for script in get_gcp_init_scripts():
            expected_path = f"gs://test-bucket/artifacts/release/1.2.3{script}"
            assert expected_path in executable_files, f"Missing required script: {script}"

    def test_custom_initialization_actions(self):
        """Verify custom initialization actions are added to default scripts."""
        custom_scripts = [
            "gs://custom-bucket/custom-script1.sh",
            "gs://custom-bucket/custom-script2.sh",
        ]

        config_json = generate_dataproc_cluster_config(
            num_workers=5,
            project_id="test-project",
            artifact_prefix="gs://test-bucket/artifacts",
            version="1.0.0",
            initialization_actions=custom_scripts,
        )
        config = json.loads(config_json)

        init_actions = config["initializationActions"]
        executable_files = [action["executable_file"] for action in init_actions]

        # Verify custom scripts are included
        for script in custom_scripts:
            assert script in executable_files

        # Verify default scripts are still included
        for script in get_gcp_init_scripts():
            expected_path = f"gs://test-bucket/artifacts/release/1.0.0{script}"
            assert expected_path in executable_files

    def test_artifact_prefix_trailing_slash_handling(self):
        """Verify artifact prefix handles trailing slashes correctly."""
        configs = []
        for prefix in ["gs://bucket/path", "gs://bucket/path/"]:
            config_json = generate_dataproc_cluster_config(
                num_workers=1,
                project_id="test-project",
                artifact_prefix=prefix,
                version="1.0.0",
            )
            configs.append(json.loads(config_json))

        # Both configs should produce identical script paths
        init_files_1 = [a["executable_file"] for a in configs[0]["initializationActions"]]
        init_files_2 = [a["executable_file"] for a in configs[1]["initializationActions"]]
        assert init_files_1 == init_files_2

    def test_version_in_script_paths(self):
        """Verify version is correctly embedded in script paths."""
        version = "2.5.10"
        config_json = generate_dataproc_cluster_config(
            num_workers=1,
            project_id="test-project",
            artifact_prefix="gs://bucket",
            version=version,
        )
        config = json.loads(config_json)

        init_actions = config["initializationActions"]
        executable_files = [action["executable_file"] for action in init_actions]

        for script in get_gcp_init_scripts():
            expected_path = f"gs://bucket/release/{version}{script}"
            assert expected_path in executable_files


class TestScriptValidation:
    """Tests to validate that GCP scripts are properly discovered and exist."""

    def test_gcp_scripts_are_discovered(self):
        """Verify that get_gcp_init_scripts() is populated with scripts."""
        assert len(get_gcp_init_scripts()) > 0, (
            "No GCP initialization scripts were discovered. "
            "Check that scripts exist in ai/chronon/repo/scripts/gcp/"
        )

        # All discovered scripts should end with .sh and have the correct path format
        for script_path in get_gcp_init_scripts():
            assert script_path.startswith("/scripts/gcp/"), (
                f"Script path {script_path} doesn't have expected prefix '/scripts/gcp/'"
            )
            assert script_path.endswith(".sh"), (
                f"Script path {script_path} doesn't end with .sh"
            )

    def test_gcp_scripts_exist_in_package(self):
        """Verify all discovered scripts actually exist in the Python package."""
        # Get the package scripts directory
        test_dir = os.path.dirname(os.path.abspath(__file__))
        python_dir = os.path.dirname(test_dir)
        package_scripts_dir = os.path.join(python_dir, "src", "ai", "chronon", "repo", "scripts", "gcp")

        assert os.path.isdir(package_scripts_dir), (
            f"GCP scripts directory not found in package: {package_scripts_dir}"
        )

        for script_path in get_gcp_init_scripts():
            script_name = os.path.basename(script_path)
            full_path = os.path.join(package_scripts_dir, script_name)

            assert os.path.isfile(full_path), (
                f"Script {script_name} was discovered but doesn't exist at {full_path}"
            )

    def test_scripts_are_sorted_deterministically(self):
        """Verify scripts are in sorted order for deterministic behavior."""
        script_names = [os.path.basename(script) for script in get_gcp_init_scripts()]
        assert script_names == sorted(script_names), (
            "Scripts should be in sorted order for deterministic behavior"
        )

    def test_specific_required_scripts_exist(self):
        """Verify specific known required scripts are present."""
        required_scripts = [
            "copy_java_security.sh",
            "opsagent_setup.sh",
        ]

        discovered_names = [os.path.basename(script) for script in get_gcp_init_scripts()]

        for required in required_scripts:
            assert required in discovered_names, (
                f"Required script {required} not found in discovered scripts: {discovered_names}"
            )


class TestFixedGcpCluster:
    """Tests for fixed_gcp_cluster t-shirt sizing function."""

    def test_small_cluster_config(self):
        """Verify small cluster configuration."""
        config_json = fixed_gcp_cluster(
            size="small",
            project_id="test-project",
            artifact_prefix="gs://test-bucket",
        )
        config = json.loads(config_json)

        assert config["workerConfig"]["numInstances"] == 20
        assert config["workerConfig"]["machineTypeUri"] == "n2-highmem-4"

    def test_medium_cluster_config(self):
        """Verify medium cluster configuration."""
        config_json = fixed_gcp_cluster(
            size="medium",
            project_id="test-project",
            artifact_prefix="gs://test-bucket",
        )
        config = json.loads(config_json)

        assert config["workerConfig"]["numInstances"] == 50
        assert config["workerConfig"]["machineTypeUri"] == "n2-highmem-16"

    def test_large_cluster_config(self):
        """Verify large cluster configuration."""
        config_json = fixed_gcp_cluster(
            size="large",
            project_id="test-project",
            artifact_prefix="gs://test-bucket",
        )
        config = json.loads(config_json)

        assert config["workerConfig"]["numInstances"] == 250
        assert config["workerConfig"]["machineTypeUri"] == "n2-highmem-16"

    def test_invalid_size_raises_error(self):
        """Verify invalid size raises ValueError."""
        with pytest.raises(ValueError, match="Invalid size"):
            fixed_gcp_cluster(
                size="xlarge",  # Invalid size
                project_id="test-project",
                artifact_prefix="gs://test-bucket",
            )


class TestEmrClusterConfig:
    """Tests for EMR cluster configuration generation."""

    def test_basic_emr_config(self):
        """Verify basic EMR cluster configuration structure."""
        config_json = generate_emr_cluster_config(
            instance_count=10,
            subnet_name="test-subnet",
            security_group_name="test-sg",
        )
        config = json.loads(config_json)

        assert config["instanceCount"] == 10
        assert config["subnetName"] == "test-subnet"
        assert config["securityGroupName"] == "test-sg"
        assert "releaseLabel" in config
        assert "instanceType" in config
        assert "autoTerminationPolicy" in config

    def test_emr_custom_instance_type(self):
        """Verify custom instance type is used."""
        config_json = generate_emr_cluster_config(
            instance_count=5,
            subnet_name="test-subnet",
            security_group_name="test-sg",
            instance_type="r5.2xlarge",
        )
        config = json.loads(config_json)

        assert config["instanceType"] == "r5.2xlarge"
