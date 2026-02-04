import logging
import os
import sys
import yaml
 
def setup_logging(app_name: str = "BI-Metadata-Extractor") -> logging.Logger:
    """Setup global logging — works for both script and .exe builds."""
    # Detect base directory (exe or script)
    if getattr(sys, "frozen", False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    
    log_file_path = None
    config_path = os.path.join(base_dir, "config", "tableau.yaml")

    # Try to load YAML config
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        log_file_path = config.get("tableau", {}).get("logging", {}).get("logfilepath")

        if not log_file_path:
            raise KeyError("Missing 'tableau.logging.logfilepath' in tableau.yaml")

    except FileNotFoundError:
        print(f"[WARNING] Config file not found at {config_path}. Using default log path.")
    except KeyError as e:
        print(f"[WARNING] {e}. Using default log path.")
    except Exception as e:
        print(f"[WARNING] Failed to read config file: {e}. Using default log path.")

    # Fallback to default logs folder if config fails
    if not log_file_path:
        log_dir = os.path.join(base_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, "app.log")

    else:
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
 
    # Remove old handlers before configuring new ones
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
 
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path, mode="a", encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ]
    )
 
    logger = logging.getLogger(app_name)
    logger.info(f" {app_name} started.")
    print(f"[DEBUG] Logging to: {log_file_path}")
    return logger