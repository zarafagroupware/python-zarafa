import zarafa
zarafa.User('user1').outbox.create_item(subject='subject', to='user1@domain.com; user2@domain.com', body='this is a body').send()
