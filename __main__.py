if __name__ == '__main__':
    import argparse
    from paqc.driver import driver

    # define command line parser
    parser = argparse.ArgumentParser(prog='paqc', description='Data QC package '
                                     'of the Predictive Analytics team.')

    # add arguments
    parser.add_argument('config_path', type=str, action='store',
                        help="Path to config YAML file.")
    parser.add_argument('--debug', action='store_true', help="Useful for devs.")
    parser.add_argument('--silent', action='store_false',
                        help="Controls verbosity. Use it if you want paqc to"
                             "run with less messages.")

    # parse input parameters
    args = parser.parse_args()

    # execute PAQC pipeline
    d = driver.Driver(args.config_path, verbose=args.silent, debug=args.debug)
    d.run()
