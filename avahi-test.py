#!/usr/bin/python2
import dbus
from dbus.mainloop.glib import DBusGMainLoop
import avahi
import gobject
import time

class AddressBook:
	'''
	The address book maintains a list of peers that are currently online
	according to Avahi. The list is stored as a document in CouchDB
	'''
	def __init__(self):
		pass

	def add_peer(self, name, host, port, type):
		'''
		Add a peer to the list of connected peers
		'''
		# Update the document in CouchDB
		
		# Refresh sync rules if needed
		self._update_synchronisation_rules()
	
	def remove_peer(self, name):
		'''
		Remove the peer with the name "name"
		'''
		# Update the document in CouchDB
		
		# Refresh sync rules if needed
		self._update_synchronisation_rules()
		
	def _update_synchronisation_rules(self):
		'''
		Check out the list of connected peers. If it contains bridges
		establish synchronisation rules with them only, otherwise make
		synchronisation rules with all the peers
		'''
		pass
	
class AvahiListener:
	'''
	This class is used to connect to the signals posted by Avahi on DBus.
	The content of the indicated addressbook is updated according to those
	signals
	'''
	def __init__(self, addressBook):
		self._addressBook = addressBook
		
		loop = DBusGMainLoop(set_as_default=True)
		self._bus = dbus.SystemBus(mainloop=loop)
		self.server = dbus.Interface(
			self._bus.get_object(avahi.DBUS_NAME, avahi.DBUS_PATH_SERVER),
			avahi.DBUS_INTERFACE_SERVER)

		# We listen only to HTTP services on the local loop
		browser = dbus.Interface(self._bus.get_object(avahi.DBUS_NAME,
				self.server.ServiceBrowserNew(avahi.IF_UNSPEC,
				avahi.PROTO_UNSPEC, "_http._tcp", 'local', dbus.UInt32(0))),
				avahi.DBUS_INTERFACE_SERVICE_BROWSER)
		
		# Connect to all the relevant signals
		browser.connect_to_signal("ItemNew", self.item_new)
		browser.connect_to_signal("ItemRemove", self.item_remove)

	def item_new(self, interface, protocol, name, stype, domain, flags):
		'''
		A new peer appeared, call the resolution service to get its
		description
		'''
		# If the service is not ERS, return directly
		if name == 'Something that is not ERS':
			return
		
		# Call the resolution service
		self.server.ResolveService(interface, protocol, name, stype,
			domain, avahi.PROTO_UNSPEC, dbus.UInt32(0),
			reply_handler=self.resolved, error_handler=self.resolve_error)

	def item_remove(self, interface, protocol, name, service, domain, flags):
		'''
		A peer is gone
		'''
		print "[Remove]", name
		self._addressBook.remove_peer(name)
		
	def resolved(self, interface, protocol, name, service, domain, host,
					aprotocol, address, port, txt, flags):
		'''
		Now that we know everything about it, add the peer
		'''
		print "[Add]", name, txt
		# Assign the type depending on the name and/or the txt
		type = None
		self._addressBook.add_peer(name, address, port, type)

	def resolve_error(self, *args, **kwargs):
		print "resolve error:", args, kwargs

def main():
	# Create an instance of the addressbook
	addressBook = AddressBook();
	browser = AvahiListener(addressBook)
	# while True:
	# 	time.sleep(3)
	# 	for key, value in browser.services.items():
	# 		print key, str(value)
	mainloop = gobject.MainLoop()
	mainloop.run()
	
if __name__ == '__main__':
	main()

# Based on http://stackoverflow.com/questions/15553508/browsing-avahi-services-with-python-misses-services
# and http://stackoverflow.com/questions/5126413/stopping-the-avahi-service-and-return-a-list-of-elements
# and the "datastore-service" daemon of Sugar

