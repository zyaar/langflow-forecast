import orjson
from langflow.base.forecasting_common.misc.forecast_gen_flow_jason import forecast_flow_generator

input_file = "./output/output_openai_json.json"
output_file = "./output/output_flow_test.json"

def load_json_file(filename: str):
    try:
        with open(filename, "rb") as f:  # orjson expects bytes
            data = orjson.loads(f.read())
        return data
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None
    except orjson.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None
    
def save_json_file(json_str: str, filename: str):
    with open(filename, "w") as f:
        f.write(json_str)
    

if __name__ == "__main__":
    json_obj = load_json_file(input_file)

    if json_obj is not None:
        generator_obj = forecast_flow_generator()

        flow_json_def = generator_obj.forecast_gen_flow_json(json_obj)

        save_json_file(flow_json_def, output_file)
        #generator_obj.test()
        