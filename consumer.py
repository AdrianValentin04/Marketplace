"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

import time
from threading import Thread

class Consumer(Thread):
    """
    Class that represents a consumer.
    """

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """

        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time
        Thread.__init__(self, **kwargs)

    def run(self):

        commands = ["add", "remove"]

        # for every cart
        for carts in self.carts:

            # assign a new cart id
            cart_id = self.marketplace.new_cart()

            # for every operation
            for op in carts:

                (prod, quan, type) = (op['product'], op['quantity'], op['type'])

                # add operatiom
                if type == commands[0]:

                    # add all the items requested
                    while quan > 0:

                        if self.marketplace.add_to_cart(cart_id, prod):
                            quan -= 1
                        else:
                            time.sleep(self.retry_wait_time)

                # remove operation
                elif type == commands[1]:

                    while quan > 0:
                        self.marketplace.remove_from_cart(cart_id, prod)
                        quan -= 1

            # order the cart
            self.marketplace.place_order(cart_id)
