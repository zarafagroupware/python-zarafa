import zarafa
zarafa.User('user1').outbox.create_item(subject='subject', to='user@test.com; user2@test.com', body='this is a body').send()
