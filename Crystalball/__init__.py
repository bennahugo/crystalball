import logging

##############################################################
# Initialize Application Logger
##############################################################


def create_logger():
    """ Create a console logger """
    log = logging.getLogger("crystalball")
    cfmt = logging.Formatter(
        ('%(name)s - %(asctime)s %(levelname)s - %(message)s'))
    log.setLevel(logging.DEBUG)
    filehandler = logging.FileHandler("crystalball.log")
    filehandler.setFormatter(cfmt)
    log.addHandler(filehandler)
    log.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(cfmt)

    log.addHandler(console)

    return log


# Create the log object
log = create_logger()