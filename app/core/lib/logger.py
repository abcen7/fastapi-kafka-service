import logging

main_logger = logging.getLogger(__name__)
main_logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s [%(levelname)s]: %(message)s")
console_handler.setFormatter(formatter)

if not main_logger.handlers:
    main_logger.addHandler(console_handler)

# main_logger.debug("Debug message")
# main_logger.info("Info message")
# main_logger.warning("Warning message")
# main_logger.error("Error message")
# main_logger.critical("Critical message")
