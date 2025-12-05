"""
Pipeline Orchestrator - Runs the complete Medallion Architecture
"""
from typing import Dict, Any
from datetime import datetime
from medallionlayer.bronze_layer import BronzeLayer
from medallionlayer.silver_layer import SilverLayer
from medallionlayer.gold_layer import GoldLayer
from utils.logger import setup_logger

logger = setup_logger(__name__)


class PipelineOrchestrator:
    """Orchestrates the complete data pipeline"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Pipeline Orchestrator

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.start_time = None
        self.end_time = None

        logger.info("Pipeline Orchestrator initialized")

    def run(self, layers: list = ['bronze', 'silver', 'gold']) -> Dict[str, Any]:
        """
        Run the complete pipeline

        Args:
            layers: List of layers to run (default: all)

        Returns:
            Dictionary with execution results
        """
        self.start_time = datetime.now()

        logger.info("\n" + "=" * 80)
        logger.info("MEDALLION ARCHITECTURE PIPELINE - STARTING")
        logger.info("=" * 80)
        logger.info(f"Start Time: {self.start_time}")
        logger.info(f"Layers to execute: {', '.join(layers)}")
        logger.info("=" * 80)

        results = {
            'status': 'success',
            'start_time': self.start_time,
            'layers': {}
        }

        try:
            # Run Bronze Layer
            if 'bronze' in layers:
                bronze = BronzeLayer(self.config)
                results['layers']['bronze'] = bronze.run()

            # Run Silver Layer
            if 'silver' in layers:
                silver = SilverLayer(self.config)
                results['layers']['silver'] = silver.run()

            # Run Gold Layer
            if 'gold' in layers:
                gold = GoldLayer(self.config)
                results['layers']['gold'] = gold.run()

            # Check for any failures
            for layer_name, layer_result in results['layers'].items():
                if layer_result['status'] != 'success':
                    results['status'] = 'partial_success'
                    break

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            results['status'] = 'failed'
            results['error'] = str(e)

        self.end_time = datetime.now()
        results['end_time'] = self.end_time
        results['duration_seconds'] = (self.end_time - self.start_time).total_seconds()

        # Print summary
        self._print_summary(results)

        return results

    def _print_summary(self, results: Dict[str, Any]):
        """Print execution summary"""
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Status: {results['status'].upper()}")
        logger.info(f"Duration: {results['duration_seconds']:.2f} seconds")

        for layer_name, layer_result in results.get('layers', {}).items():
            logger.info(f"\n{layer_name.upper()} Layer:")
            logger.info(f"  Status: {layer_result['status']}")

            if 'tables_processed' in layer_result:
                logger.info(f"  Tables Processed: {layer_result['tables_processed']}")
            if 'aggregations_processed' in layer_result:
                logger.info(f"  Aggregations: {layer_result['aggregations_processed']}")
            if 'total_rows' in layer_result:
                logger.info(f"  Total Rows: {layer_result['total_rows']}")
            if layer_result.get('errors'):
                logger.info(f"  Errors: {len(layer_result['errors'])}")
                for error in layer_result['errors']:
                    logger.error(f"    - {error}")

        logger.info("=" * 80 + "\n")