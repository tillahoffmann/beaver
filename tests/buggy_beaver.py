import beaver_build as bb


# Create a file and then error out.
bb.Shell("output.txt", None, "echo hello > $@ && false")
