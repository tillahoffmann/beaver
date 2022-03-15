from beaver import transformations as bt


# Generate some arbitrary inputs.
inputs = bt.Sleep(["input1.txt", "input2.txt"], None, time=0)

# Concatenate the inputs.
output = bt.Shell("output.txt", inputs, "cat $^ > $@")
