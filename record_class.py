class Record:
	def __init__(self):
		self.log_id = -1
		self.record_id = -1
		self.copy_set = []
		self.data = ""

	def set_copy_set(copy_set):
		self.copy_set = copy_set

	def set_record_id(record_id):
		self.record_id = record_id