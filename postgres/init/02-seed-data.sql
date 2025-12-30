-- ============================================================================
-- Dataflix - Seed Data
-- ============================================================================

-- Insert Users
INSERT INTO users (username, email, first_name, last_name, city, state, country, age) VALUES
('john_doe', 'john@example.com', 'John', 'Doe', 'São Paulo', 'SP', 'Brazil', 28),
('jane_smith', 'jane@example.com', 'Jane', 'Smith', 'Rio de Janeiro', 'RJ', 'Brazil', 32),
('carlos_silva', 'carlos@example.com', 'Carlos', 'Silva', 'Belo Horizonte', 'MG', 'Brazil', 25),
('maria_santos', 'maria@example.com', 'Maria', 'Santos', 'Curitiba', 'PR', 'Brazil', 29),
('pedro_oliveira', 'pedro@example.com', 'Pedro', 'Oliveira', 'Salvador', 'BA', 'Brazil', 35),
('ana_costa', 'ana@example.com', 'Ana', 'Costa', 'Brasília', 'DF', 'Brazil', 27),
('lucas_martins', 'lucas@example.com', 'Lucas', 'Martins', 'Recife', 'PE', 'Brazil', 31),
('sophia_alves', 'sophia@example.com', 'Sophia', 'Alves', 'Manaus', 'AM', 'Brazil', 26);

-- Inserir Filmes
INSERT INTO movies (title, description, genre, release_year, director, duration_minutes, imdb_rating) VALUES
('The Shawshank Redemption', 'Two imprisoned men bond over a number of years, finding solace and eventual redemption through acts of common decency.', 'Drama', 1994, 'Frank Darabont', 142, 9.3),
('The Godfather', 'The aging patriarch of an organized crime dynasty transfers control of his clandestine empire to his reluctant youngest son.', 'Crime', 1972, 'Francis Ford Coppola', 175, 9.2),
('The Dark Knight', 'When the menace known as the Joker wreaks havoc and chaos on the people of Gotham, Batman must accept one of the greatest psychological and physical tests.', 'Action', 2008, 'Christopher Nolan', 152, 9.0),
('Pulp Fiction', 'The lives of two mob hitmen, a boxer, a gangster and his wife intertwine in four tales of violence and redemption.', 'Crime', 1994, 'Quentin Tarantino', 154, 8.9),
('Forrest Gump', 'The presidencies of Kennedy and Johnson, the Vietnam War, the Watergate scandal and other historical events unfold from the perspective of an Alabama man with an IQ of 75.', 'Drama', 1994, 'Robert Zemeckis', 142, 8.8),
('Inception', 'A thief who steals corporate secrets through dream-sharing technology is given the inverse task of planting an idea into the mind of a C.E.O.', 'Sci-Fi', 2010, 'Christopher Nolan', 148, 8.8),
('The Matrix', 'A computer hacker learns from mysterious rebels about the true nature of his reality and his role in the war against its controllers.', 'Sci-Fi', 1999, 'The Wachowskis', 136, 8.7),
('Goodfellas', 'The story of Henry Hill and his life in the mafia, covering his relationship with his wife Karen Hill and his mob partners.', 'Crime', 1990, 'Martin Scorsese', 146, 8.7),
('The Silence of the Lambs', 'A young FBI cadet must receive the help of an incarcerated cannibal killer to catch another serial killer who skins his victims.', 'Thriller', 1991, 'Jonathan Demme', 118, 8.6),
('Saving Private Ryan', 'Following the Normandy Landings, a group of U.S. soldiers go behind enemy lines to retrieve a paratrooper.', 'War', 1998, 'Steven Spielberg', 169, 8.6),
('Interstellar', 'A team of explorers travel through a wormhole in space in an attempt to ensure humanity''s survival.', 'Sci-Fi', 2014, 'Christopher Nolan', 169, 8.6),
('The Usual Suspects', 'A sole survivor tells of the twisty events leading up to a horrific gun battle on a boat.', 'Crime', 1995, 'Bryan Singer', 106, 8.5),
('Gladiator', 'A former Roman General sets out to exact vengeance against the corrupt emperor who murdered his family and sent him into slavery.', 'Action', 2000, 'Ridley Scott', 155, 8.5),
('The Green Mile', 'The lives of guards on Death Row are affected by one of their charges: a black man accused of child murder and rape, yet who has a mysterious gift.', 'Drama', 1999, 'Frank Darabont', 189, 8.6),
('Jurassic Park', 'A pragmatic paleontologist touring an almost complete theme park is tasked with protecting a couple of kids.', 'Sci-Fi', 1993, 'Steven Spielberg', 127, 8.2),
('Avatar', 'A paraplegic Marine dispatched to the moon Pandora on a unique mission becomes torn between following his orders and protecting the world he feels is his home.', 'Sci-Fi', 2009, 'James Cameron', 162, 7.8),
('The Avengers', 'Earth''s mightiest heroes must come together and learn to fight as a team to defend the world from an alien threat.', 'Action', 2012, 'Joss Whedon', 143, 8.0),
('Titanic', 'A seventeen-year-old aristocrat falls in love with a kind but poor artist aboard the luxurious, ill-fated R.M.S. Titanic.', 'Romance', 1997, 'James Cameron', 194, 7.8),
('The Lion King', 'Lion prince Simba and his father are targeted by his bitter uncle, who wants to ascend the throne himself.', 'Animation', 1994, 'Roger Allers', 88, 8.5),
('Toy Story', 'A cowboy doll is profoundly threatened when a new spaceman figure supplants him as top toy in a boy''s bedroom.', 'Animation', 1995, 'John Lasseter', 81, 8.3);

-- Insert Watched Movies
INSERT INTO watched_movies (user_id, movie_id) VALUES
(1, 1), (1, 2), (1, 3), (1, 4), (1, 5),
(2, 1), (2, 6), (2, 7), (2, 8),
(3, 3), (3, 9), (3, 10), (3, 11),
(4, 1), (4, 2), (4, 12), (4, 13),
(5, 4), (5, 8), (5, 14), (5, 15),
(6, 6), (6, 7), (6, 16), (6, 17),
(7, 2), (7, 5), (7, 18), (7, 19),
(8, 1), (8, 3), (8, 20);

-- Insert Ratings
INSERT INTO ratings (user_id, movie_id, rating, liked) VALUES
(1, 1, 5, true), (1, 2, 5, true), (1, 3, 5, true), (1, 4, 4, true), (1, 5, 4, true),
(2, 1, 5, true), (2, 6, 5, true), (2, 7, 4, true), (2, 8, 4, true),
(3, 3, 5, true), (3, 9, 4, true), (3, 10, 5, true), (3, 11, 5, true),
(4, 1, 5, true), (4, 2, 5, true), (4, 12, 3, false), (4, 13, 4, true),
(5, 4, 5, true), (5, 8, 4, true), (5, 14, 5, true), (5, 15, 3, false),
(6, 6, 5, true), (6, 7, 5, true), (6, 16, 3, false), (6, 17, 4, true),
(7, 2, 4, true), (7, 5, 5, true), (7, 18, 3, false), (7, 19, 4, true),
(8, 1, 5, true), (8, 3, 4, true), (8, 20, 5, true);
