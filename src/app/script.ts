const pgp = require('pg-promise')();
const connectionString = 'postgres:postgres:2001@localhost:5432/movie_ratings';
const db = pgp(connectionString);

// a. Top 5 Movie Titles
const topMoviesDurationQuery = 'SELECT * FROM movies ORDER BY minutes DESC LIMIT 5;';
const topMoviesYearQuery = 'SELECT * FROM movies ORDER BY year DESC LIMIT 5;';
const topMoviesAvgRatingQuery = 'SELECT * FROM movies WHERE num_ratings >= 5 ORDER BY avg_rating DESC LIMIT 5;';
const topMoviesNumRatingsQuery = 'SELECT * FROM movies ORDER BY num_ratings DESC LIMIT 5;';

// b. Number of Unique Raters
const uniqueRatersCountQuery = 'SELECT COUNT(DISTINCT rater_id) FROM ratings;';

// c. Top 5 Rater IDs
const topRatersMoviesRatedQuery = 'SELECT rater_id, COUNT(*) as movies_rated FROM ratings GROUP BY rater_id ORDER BY movies_rated DESC LIMIT 5;';
const topRatersAvgRatingQuery = 'SELECT rater_id, AVG(rating) as avg_rating FROM ratings GROUP BY rater_id HAVING COUNT(*) >= 5 ORDER BY avg_rating DESC LIMIT 5;';

// d. Top Rated Movie
const topRatedMovieDirectorQuery = `
    SELECT m.title, r.rating
    FROM movies m
    JOIN ratings r ON m.id = r.movie_id
    WHERE m.director = 'Michael Bay'
    ORDER BY r.rating DESC
    LIMIT 1;
`;
const topRatedMovieComedy2013IndiaQuery = `
    SELECT m.title, r.rating
    FROM movies m
    JOIN ratings r ON m.id = r.movie_id
    WHERE m.genre = 'Comedy' AND m.year = 2013 AND m.country = 'India' AND m.num_ratings >= 5
    ORDER BY r.rating DESC
    LIMIT 1;
`;

// e. Favorite Movie Genre of Rater ID 1040
const rater1040FavoriteGenreQuery = 'SELECT genre FROM ratings WHERE rater_id = 1040 GROUP BY genre ORDER BY COUNT(*) DESC LIMIT 1;';

// f. Highest Average Rating for a Movie Genre by Rater ID 1040
const rater1040GenreAvgRatingQuery = `
    SELECT genre, AVG(rating) as avg_rating
    FROM ratings
    WHERE rater_id = 1040
    GROUP BY genre
    HAVING COUNT(*) >= 5
    ORDER BY avg_rating DESC
    LIMIT 1;
`;

// g. Year with Second-Highest Number of Action Movies
const secondHighestActionYearQuery = `
    SELECT year, COUNT(*) as num_action_movies
    FROM movies
    WHERE genre = 'Action' AND country = 'USA' AND avg_rating >= 6.5 AND minutes < 120
    GROUP BY year
    ORDER BY num_action_movies DESC
    LIMIT 1 OFFSET 1;
`;

// h. Count of Movies with High Ratings
const highRatedMoviesCountQuery = 'SELECT COUNT(*) FROM movies WHERE num_ratings >= 5 AND avg_rating >= 7;';

// Execute queries
Promise.all([
    db.any(topMoviesDurationQuery),
    db.any(topMoviesYearQuery),
    db.any(topMoviesAvgRatingQuery),
    db.any(topMoviesNumRatingsQuery),
    db.one(uniqueRatersCountQuery, [], (a: { count: string | number; }) => +a.count),
    db.any(topRatersMoviesRatedQuery),
    db.any(topRatersAvgRatingQuery),
    db.one(topRatedMovieDirectorQuery),
    db.one(topRatedMovieComedy2013IndiaQuery),
    db.one(rater1040FavoriteGenreQuery),
    db.one(rater1040GenreAvgRatingQuery),
    db.one(secondHighestActionYearQuery),
    db.one(highRatedMoviesCountQuery, [], (a: { count: string | number; }) => +a.count)
])
.then(results => {
    const [
        topMoviesDuration,
        topMoviesYear,
        topMoviesAvgRating,
        topMoviesNumRatings,
        uniqueRatersCount,
        topRatersMoviesRated,
        topRatersAvgRating,
        topRatedMovieDirector,
        topRatedMovieComedy2013India,
        rater1040FavoriteGenre,
        rater1040GenreAvgRating,
        secondHighestActionYear,
        highRatedMoviesCount
    ] = results;

     // Print the results
     console.log('a. Top 5 Movie Titles by Duration:\n', topMoviesDuration);
     console.log('\nb. Top 5 Movie Titles by Year of Release:\n', topMoviesYear);
     console.log('\nc. Top 5 Movie Titles by Average Rating:\n', topMoviesAvgRating);
     console.log('\nd. Top 5 Movie Titles by Number of Ratings:\n', topMoviesNumRatings);

     console.log('\nb. Number of Unique Raters:', uniqueRatersCount);

     console.log('\nc. Top 5 Rater IDs by Most Movies Rated:\n', topRatersMoviesRated);
     console.log('\nc. Top 5 Rater IDs by Highest Average Rating Given:\n', topRatersAvgRating);

     console.log('\nd. Top Rated Movie by Director "Michael Bay":\n', topRatedMovieDirector);
     console.log('\nd. Top Rated Comedy Movie in 2013 in India:\n', topRatedMovieComedy2013India);

     console.log('\ne. Favorite Movie Genre of Rater ID 1040:', rater1040FavoriteGenre);

     console.log('\nf. Highest Average Rating for a Movie Genre by Rater ID 1040:', rater1040GenreAvgRating);

     console.log('\ng. Year with Second-Highest Number of Action Movies:\n', secondHighestActionYear);
     console.log('\nh. Count of Movies with High Ratings:', highRatedMoviesCount);

     // Close the database connection
     pgp.end();
 })
 .catch(error => {
     console.error('Error:', error);
 });
db.$pool.end(); 