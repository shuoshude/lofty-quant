from quant.config import QuantConfig, load_config

def main():
    print("Hello from lofty-quant!")

    config: QuantConfig = load_config()
    print(config.paths.raw_dir)

if __name__ == "__main__":
    main()
