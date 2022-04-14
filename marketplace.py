"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from threading import Lock, currentThread

class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """
    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """

        # size for every producer
        self.queue_size_per_producer = queue_size_per_producer

        # dictionary of carts, each containing items and their quantity
        self.carts = dict()

        # dictionary of slots available
        self.storage = dict()

        # dictionary of (product, producer_id), quantity
        self.products = dict()

        # producers' id
        self.last_producer = -1

        # carts' id
        self.last_cart = -1

        # lock for producer actions
        self.lock = Lock()


    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        with self.lock:

            self.last_producer += 1
            self.storage.update({self.last_producer: self.queue_size_per_producer})

        # return the producer's id
        return self.last_producer

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """

        with self.lock:

            entry = (product, producer_id)

            # check if the producer has space
            if self.storage.get(producer_id) == 0:
                return False
            else:
                # add product to dictionary
                if entry not in list(self.products.keys()):
                    self.products[entry] = 1
                else:
                    self.products[entry] += 1

                # occupy the space
                self.storage[producer_id] -= 1
                return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        with self.lock:

            # add another cart
            self.last_cart += 1

            # assign it a dictionary
            self.carts[self.last_cart] = {}

        return self.last_cart

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """

        with self.lock:

            # check if the product exists
            for (prod, producer_id) in list(self.products.keys()):

                if prod == product and self.products.get((prod, producer_id)) > 0:

                    # add it to cart
                    if prod not in self.carts[cart_id]:
                        self.carts[cart_id][prod] = 1
                    else:
                        self.carts[cart_id][prod] += 1

                    # release the space used
                    self.storage[producer_id] += 1

                    # remove it from products
                    self.products[(prod, producer_id)] -= 1

                    return True

        return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """

        with self.lock:

            # check if the product is not in cart
            if product in self.carts[cart_id]:

                # remove the product
                if self.carts.get((cart_id, product)) != 1:
                    self.carts[cart_id][product] -= 1
                else:
                    del self.carts[cart_id][product]

                for (prod, producer_id) in list(self.products.keys()):

                    # add it back in products dictionary
                    if prod == product:
                        self.products[(prod, producer_id)] += 1
                        self.storage[producer_id] -= 1
                        break
            else:
                return

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        with self.lock:

            # print the contents of the cart
            for product, quantity in self.carts[cart_id].items():

                while quantity != 0:

                    print(currentThread().getName() + " bought ", end='')
                    print(product)
                    quantity -= 1