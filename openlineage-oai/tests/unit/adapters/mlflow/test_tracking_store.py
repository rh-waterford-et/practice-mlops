"""Unit tests for OpenLineage Tracking Store."""

import os
from unittest.mock import MagicMock, patch

import pytest


class TestOpenLineageTrackingStoreInit:
    """Tests for tracking store initialization."""

    @patch("openlineage_oai.adapters.mlflow.tracking_store.OpenLineageEmitter")
    @patch(
        "openlineage_oai.adapters.mlflow.tracking_store.OpenLineageTrackingStore._create_delegate_store"
    )
    def test_init_with_postgresql_uri(self, mock_create_delegate, mock_emitter):
        """Test initialization with PostgreSQL URI."""
        mock_create_delegate.return_value = MagicMock()

        from openlineage_oai.adapters.mlflow.tracking_store import OpenLineageTrackingStore

        os.environ["OPENLINEAGE_URL"] = "http://marquez:5000"

        store = OpenLineageTrackingStore(
            store_uri="openlineage+postgresql://user:pass@localhost:5432/mlflow",
            artifact_uri="/artifacts",
        )

        assert store._backend_uri == "postgresql://user:pass@localhost:5432/mlflow"
        mock_create_delegate.assert_called_once()

    @patch("openlineage_oai.adapters.mlflow.tracking_store.OpenLineageEmitter")
    @patch(
        "openlineage_oai.adapters.mlflow.tracking_store.OpenLineageTrackingStore._create_delegate_store"
    )
    def test_init_with_http_uri(self, mock_create_delegate, mock_emitter):
        """Test initialization with HTTP URI."""
        mock_create_delegate.return_value = MagicMock()

        from openlineage_oai.adapters.mlflow.tracking_store import OpenLineageTrackingStore

        os.environ["OPENLINEAGE_URL"] = "http://marquez:5000"

        store = OpenLineageTrackingStore(
            store_uri="openlineage+http://mlflow-server:5000",
        )

        assert store._backend_uri == "http://mlflow-server:5000"


class TestCreateDelegateStore:
    """Tests for delegate store creation."""

    @patch("openlineage_oai.adapters.mlflow.tracking_store.OpenLineageEmitter")
    def test_creates_rest_store_for_http(self, mock_emitter):
        """Test REST store creation for HTTP URIs."""
        from openlineage_oai.adapters.mlflow.tracking_store import OpenLineageTrackingStore

        os.environ["OPENLINEAGE_URL"] = "http://marquez:5000"

        with patch("mlflow.store.tracking.rest_store.RestStore") as mock_rest:
            mock_rest.return_value = MagicMock()

            OpenLineageTrackingStore(
                store_uri="openlineage+http://localhost:5000",
            )

            # RestStore should have been created
            assert mock_rest.called

    @patch("openlineage_oai.adapters.mlflow.tracking_store.OpenLineageEmitter")
    def test_creates_sql_store_for_postgresql(self, mock_emitter):
        """Test SQL store creation for PostgreSQL URIs."""
        from openlineage_oai.adapters.mlflow.tracking_store import OpenLineageTrackingStore

        os.environ["OPENLINEAGE_URL"] = "http://marquez:5000"

        with patch("mlflow.store.tracking.sqlalchemy_store.SqlAlchemyStore") as mock_sql:
            mock_sql.return_value = MagicMock()

            OpenLineageTrackingStore(
                store_uri="openlineage+postgresql://localhost/mlflow",
            )

            mock_sql.assert_called_once()

    @patch("openlineage_oai.adapters.mlflow.tracking_store.OpenLineageEmitter")
    def test_creates_file_store_for_file_uri(self, mock_emitter):
        """Test file store creation for file URIs."""
        from openlineage_oai.adapters.mlflow.tracking_store import OpenLineageTrackingStore

        os.environ["OPENLINEAGE_URL"] = "http://marquez:5000"

        with patch("mlflow.store.tracking.file_store.FileStore") as mock_file:
            mock_file.return_value = MagicMock()

            OpenLineageTrackingStore(
                store_uri="openlineage+file:///tmp/mlruns",
            )

            mock_file.assert_called_once()


class TestRunLifecycle:
    """Tests for run lifecycle methods."""

    @pytest.fixture
    def mock_store(self):
        """Create a mock tracking store for testing."""
        with patch(
            "openlineage_oai.adapters.mlflow.tracking_store.OpenLineageEmitter"
        ) as mock_emitter_cls:
            with patch(
                "openlineage_oai.adapters.mlflow.tracking_store.OpenLineageTrackingStore._create_delegate_store"
            ) as mock_create:
                mock_delegate = MagicMock()
                mock_create.return_value = mock_delegate

                mock_emitter = MagicMock()
                mock_emitter_cls.return_value = mock_emitter

                from openlineage_oai.adapters.mlflow.tracking_store import OpenLineageTrackingStore

                os.environ["OPENLINEAGE_URL"] = "http://marquez:5000"

                store = OpenLineageTrackingStore(
                    store_uri="openlineage+postgresql://localhost/mlflow",
                )

                yield store, mock_delegate, mock_emitter

    def test_create_run_emits_start(self, mock_store):
        """Test create_run emits START event."""
        store, mock_delegate, mock_emitter = mock_store

        # Setup mock run
        mock_run = MagicMock()
        mock_run.info.run_id = "run-123"
        mock_delegate.create_run.return_value = mock_run

        mock_experiment = MagicMock()
        mock_experiment.name = "test-experiment"
        mock_delegate.get_experiment.return_value = mock_experiment

        # Create run
        store.create_run(
            experiment_id="exp-1",
            user_id="user-1",
            start_time=1000,
            tags=[],
            run_name="my-run",
        )

        # Verify delegate was called
        mock_delegate.create_run.assert_called_once()

        # Verify START event was emitted
        mock_emitter.emit_start.assert_called_once()
        call_args = mock_emitter.emit_start.call_args
        assert call_args[1]["run_id"] == "run-123"

        # Verify run state was initialized
        assert "run-123" in store._run_states

    def test_update_run_info_emits_complete_on_finish(self, mock_store):
        """Test update_run_info emits COMPLETE for finished runs."""
        store, mock_delegate, mock_emitter = mock_store

        # Initialize run state
        from openlineage_oai.adapters.mlflow.tracking_store import RunState

        store._run_states["run-456"] = RunState(
            experiment_id="exp-1",
            experiment_name="test",
            run_name="test-run",
            job_name="mlflow/exp-1/test-run",
            params={"lr": "0.01"},
            metrics={"accuracy": 0.95},
            tags={},
            inputs=[],
            outputs=[],
        )

        # Mock RunStatus
        with patch("mlflow.entities.RunStatus") as mock_status:
            mock_status.FINISHED = "FINISHED"

            store.update_run_info(
                run_id="run-456",
                run_status=mock_status.FINISHED,
                end_time=2000,
                run_name="test-run",
            )

        # Verify COMPLETE event was emitted
        mock_emitter.emit_complete.assert_called_once()
        call_args = mock_emitter.emit_complete.call_args
        assert call_args[1]["run_id"] == "run-456"

        # Verify run state was cleaned up
        assert "run-456" not in store._run_states

    def test_update_run_info_emits_fail_on_failure(self, mock_store):
        """Test update_run_info emits FAIL for failed runs."""
        store, mock_delegate, mock_emitter = mock_store

        # Initialize run state
        from openlineage_oai.adapters.mlflow.tracking_store import RunState

        store._run_states["run-789"] = RunState(
            experiment_id="exp-2",
            experiment_name="failed-exp",
            run_name="failed-run",
            job_name="mlflow/exp-2/failed-run",
            params={},
            metrics={},
            tags={"mlflow.note.content": "Training crashed"},
            inputs=[],
            outputs=[],
        )

        # Mock RunStatus
        with patch("mlflow.entities.RunStatus") as mock_status:
            mock_status.FINISHED = "FINISHED"
            mock_status.FAILED = "FAILED"

            store.update_run_info(
                run_id="run-789",
                run_status=mock_status.FAILED,
                end_time=2000,
                run_name="failed-run",
            )

        # Verify FAIL event was emitted
        mock_emitter.emit_fail.assert_called_once()


class TestParamMetricLogging:
    """Tests for parameter and metric logging methods."""

    @pytest.fixture
    def mock_store_with_run(self):
        """Create a mock store with an active run."""
        with patch("openlineage_oai.adapters.mlflow.tracking_store.OpenLineageEmitter"):
            with patch(
                "openlineage_oai.adapters.mlflow.tracking_store.OpenLineageTrackingStore._create_delegate_store"
            ) as mock_create:
                mock_delegate = MagicMock()
                mock_create.return_value = mock_delegate

                from openlineage_oai.adapters.mlflow.tracking_store import (
                    OpenLineageTrackingStore,
                    RunState,
                )

                os.environ["OPENLINEAGE_URL"] = "http://marquez:5000"

                store = OpenLineageTrackingStore(
                    store_uri="openlineage+postgresql://localhost/mlflow",
                )

                # Initialize a run
                store._run_states["run-abc"] = RunState(
                    experiment_id="exp-1",
                    experiment_name="test",
                    run_name="test-run",
                    job_name="mlflow/exp-1/test-run",
                )

                yield store, mock_delegate

    def test_log_param_accumulates(self, mock_store_with_run):
        """Test log_param accumulates parameter."""
        store, mock_delegate = mock_store_with_run

        param = MagicMock()
        param.key = "learning_rate"
        param.value = "0.001"

        store.log_param("run-abc", param)

        # Verify delegate was called
        mock_delegate.log_param.assert_called_once_with("run-abc", param)

        # Verify accumulation
        assert store._run_states["run-abc"].params["learning_rate"] == "0.001"

    def test_log_params_accumulates_multiple(self, mock_store_with_run):
        """Test log_params accumulates multiple parameters."""
        store, mock_delegate = mock_store_with_run
        mock_delegate.log_params = MagicMock()

        params = [MagicMock(key="p1", value="v1"), MagicMock(key="p2", value="v2")]

        store.log_params("run-abc", params)

        # Verify accumulation
        assert store._run_states["run-abc"].params["p1"] == "v1"
        assert store._run_states["run-abc"].params["p2"] == "v2"

    def test_log_metric_accumulates(self, mock_store_with_run):
        """Test log_metric accumulates metric."""
        store, mock_delegate = mock_store_with_run

        metric = MagicMock()
        metric.key = "accuracy"
        metric.value = 0.95

        store.log_metric("run-abc", metric)

        # Verify delegate was called
        mock_delegate.log_metric.assert_called_once_with("run-abc", metric)

        # Verify accumulation
        assert store._run_states["run-abc"].metrics["accuracy"] == 0.95

    def test_log_metrics_accumulates_multiple(self, mock_store_with_run):
        """Test log_metrics accumulates multiple metrics."""
        store, mock_delegate = mock_store_with_run
        mock_delegate.log_metrics = MagicMock()

        metrics = [
            MagicMock(key="accuracy", value=0.95),
            MagicMock(key="loss", value=0.05),
        ]

        store.log_metrics("run-abc", metrics)

        # Verify accumulation
        assert store._run_states["run-abc"].metrics["accuracy"] == 0.95
        assert store._run_states["run-abc"].metrics["loss"] == 0.05

    def test_set_tag_accumulates(self, mock_store_with_run):
        """Test set_tag accumulates tag."""
        store, mock_delegate = mock_store_with_run

        tag = MagicMock()
        tag.key = "model_type"
        tag.value = "random_forest"

        store.set_tag("run-abc", tag)

        # Verify delegate was called
        mock_delegate.set_tag.assert_called_once()

        # Verify accumulation
        assert store._run_states["run-abc"].tags["model_type"] == "random_forest"


class TestInputLogging:
    """Tests for input dataset logging."""

    @pytest.fixture
    def mock_store_with_run(self):
        """Create a mock store with an active run."""
        with patch(
            "openlineage_oai.adapters.mlflow.tracking_store.OpenLineageEmitter"
        ) as mock_emitter_cls:
            with patch(
                "openlineage_oai.adapters.mlflow.tracking_store.OpenLineageTrackingStore._create_delegate_store"
            ) as mock_create:
                mock_delegate = MagicMock()
                mock_create.return_value = mock_delegate

                mock_emitter = MagicMock()
                mock_emitter_cls.return_value = mock_emitter

                from openlineage_oai.adapters.mlflow.tracking_store import (
                    OpenLineageTrackingStore,
                    RunState,
                )

                os.environ["OPENLINEAGE_URL"] = "http://marquez:5000"

                store = OpenLineageTrackingStore(
                    store_uri="openlineage+postgresql://localhost/mlflow",
                )

                # Initialize a run
                store._run_states["run-input"] = RunState(
                    experiment_id="exp-1",
                    experiment_name="test",
                    run_name="test-run",
                    job_name="mlflow/exp-1/test-run",
                )

                yield store, mock_delegate, mock_emitter

    def test_log_inputs_accumulates_datasets(self, mock_store_with_run):
        """Test log_inputs accumulates input datasets."""
        store, mock_delegate, mock_emitter = mock_store_with_run

        # Create mock dataset
        mock_source = MagicMock()
        mock_source.to_dict.return_value = {"type": "pandas", "uri": "memory://iris"}

        mock_dataset = MagicMock()
        mock_dataset.name = "iris_data"
        mock_dataset.digest = "abc123"
        mock_dataset.source = mock_source
        del mock_dataset.schema

        mock_input = MagicMock()
        mock_input.dataset = mock_dataset
        mock_input.context = "training"

        store.log_inputs("run-input", datasets=[mock_input])

        # Verify inputs were accumulated
        assert len(store._run_states["run-input"].inputs) == 1
        assert store._run_states["run-input"].inputs[0]["name"] == "iris_data"


class TestDelegateMethods:
    """Tests for methods that purely delegate to the real store."""

    @pytest.fixture
    def mock_store(self):
        """Create a mock tracking store."""
        with patch("openlineage_oai.adapters.mlflow.tracking_store.OpenLineageEmitter"):
            with patch(
                "openlineage_oai.adapters.mlflow.tracking_store.OpenLineageTrackingStore._create_delegate_store"
            ) as mock_create:
                mock_delegate = MagicMock()
                mock_create.return_value = mock_delegate

                from openlineage_oai.adapters.mlflow.tracking_store import OpenLineageTrackingStore

                os.environ["OPENLINEAGE_URL"] = "http://marquez:5000"

                store = OpenLineageTrackingStore(
                    store_uri="openlineage+postgresql://localhost/mlflow",
                )

                yield store, mock_delegate

    def test_get_experiment_delegates(self, mock_store):
        """Test get_experiment delegates to real store."""
        store, mock_delegate = mock_store

        mock_experiment = MagicMock()
        mock_delegate.get_experiment.return_value = mock_experiment

        result = store.get_experiment("exp-1")

        mock_delegate.get_experiment.assert_called_once_with("exp-1")
        assert result == mock_experiment

    def test_list_experiments_delegates(self, mock_store):
        """Test list_experiments delegates to real store."""
        store, mock_delegate = mock_store

        mock_experiments = [MagicMock(), MagicMock()]
        mock_delegate.list_experiments.return_value = mock_experiments

        result = store.list_experiments()

        mock_delegate.list_experiments.assert_called_once()
        assert result == mock_experiments

    def test_get_run_delegates(self, mock_store):
        """Test get_run delegates to real store."""
        store, mock_delegate = mock_store

        mock_run = MagicMock()
        mock_delegate.get_run.return_value = mock_run

        result = store.get_run("run-123")

        mock_delegate.get_run.assert_called_once_with("run-123")
        assert result == mock_run

    def test_search_runs_delegates(self, mock_store):
        """Test search_runs delegates to real store."""
        store, mock_delegate = mock_store

        mock_results = MagicMock()
        mock_delegate.search_runs.return_value = mock_results

        result = store.search_runs(
            experiment_ids=["exp-1"],
            filter_string="status = 'FINISHED'",
            run_view_type=None,
        )

        mock_delegate.search_runs.assert_called_once()
        assert result == mock_results

    def test_create_experiment_delegates(self, mock_store):
        """Test create_experiment delegates to real store."""
        store, mock_delegate = mock_store

        mock_delegate.create_experiment.return_value = "new-exp-id"

        result = store.create_experiment(
            name="new-experiment",
            artifact_location="/artifacts",
            tags=[],
        )

        mock_delegate.create_experiment.assert_called_once()
        assert result == "new-exp-id"

    def test_delete_run_delegates(self, mock_store):
        """Test delete_run delegates to real store."""
        store, mock_delegate = mock_store

        store.delete_run("run-to-delete")

        mock_delegate.delete_run.assert_called_once_with("run-to-delete")

    def test_restore_run_delegates(self, mock_store):
        """Test restore_run delegates to real store."""
        store, mock_delegate = mock_store

        store.restore_run("run-to-restore")

        mock_delegate.restore_run.assert_called_once_with("run-to-restore")


class TestRunState:
    """Tests for RunState dataclass."""

    def test_run_state_defaults(self):
        """Test RunState has correct default values."""
        from openlineage_oai.adapters.mlflow.tracking_store import RunState

        state = RunState(experiment_id="exp-1")

        assert state.experiment_id == "exp-1"
        assert state.experiment_name == ""
        assert state.run_name == ""
        assert state.job_name == ""
        assert state.params == {}
        assert state.metrics == {}
        assert state.tags == {}
        assert state.inputs == []
        assert state.outputs == []

    def test_run_state_with_values(self):
        """Test RunState with populated values."""
        from openlineage_oai.adapters.mlflow.tracking_store import RunState

        state = RunState(
            experiment_id="exp-2",
            experiment_name="my-experiment",
            run_name="training-run",
            job_name="mlflow/exp-2/training-run",
            params={"lr": "0.01"},
            metrics={"acc": 0.95},
            tags={"model": "rf"},
            inputs=[{"name": "data"}],
            outputs=[{"name": "model"}],
        )

        assert state.experiment_name == "my-experiment"
        assert state.params["lr"] == "0.01"
        assert state.metrics["acc"] == 0.95
        assert len(state.inputs) == 1
        assert len(state.outputs) == 1
